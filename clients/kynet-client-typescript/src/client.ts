import { GatewayRequestError, KynetError, aborted, asError } from "./errors.js";
import { actionKey, GatewayPacketCodec, packet, PacketType, splitMessageId } from "./protocol.js";
import type { GatewayPacket, PacketLimits } from "./protocol.js";
import { delay, parseEndpoint, WebSocketTransport, withDeadline } from "./transport.js";
import type { GatewayTransport, TransportFactory } from "./transport.js";

export type ClientState = "disconnected" | "connecting" | "recovering" | "connected" | "reconnecting";
export interface ClientSession { readonly sessionId: bigint; readonly userId: bigint; readonly isAuthenticated: boolean }
export interface RequestOptions { timeoutMs?: number; signal?: AbortSignal }
export interface ClientOptions extends Partial<PacketLimits> {
  endpoint?: string;
  connectTimeoutMs?: number;
  requestTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  recoveryTimeoutMs?: number;
  autoReconnect?: boolean;
  reconnectInitialDelayMs?: number;
  reconnectMaxDelayMs?: number;
  maxPendingRequests?: number;
  maxPendingWrites?: number;
  transportFactory?: TransportFactory;
  sessionRecoveryHandler?: (client: KynetClient, signal: AbortSignal) => Promise<void> | void;
}

export interface ClientEvents {
  stateChanged: ClientState;
  sessionChanged: ClientSession;
  push: GatewayPacket;
  error: Error;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  let settled = false;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  // A handshake or connection can fail before the caller attaches an await.
  void promise.catch(() => {});
  return { promise, get settled() { return settled; },
    resolve(value: T) { if (!settled) { settled = true; resolve(value); } },
    reject(error: Error) { if (!settled) { settled = true; reject(error); } } };
}
type Deferred<T> = ReturnType<typeof deferred<T>>;
interface Pending { completion: Deferred<GatewayPacket>; type: PacketType; cmd: number; act: number }
interface Attempt {
  transport: GatewayTransport;
  stop: AbortController;
  closed: Deferred<never>;
  pending: Map<bigint, Pending>;
  writeTail: Promise<void>;
  writes: number;
}

const session = (sessionId = 0n, userId = 0n): ClientSession => Object.freeze({ sessionId, userId, isAuthenticated: userId > 0n });

export class ActionResponse {
  constructor(readonly packet: GatewayPacket) {}
  get ok(): boolean { return this.packet.errorCode === null; }
  get payload(): Uint8Array | null { return this.packet.payload; }
  get errorCode(): string | null { return this.packet.errorCode; }
  get errorMessage(): string | null { return this.packet.errorMessage; }
  get sessionId(): bigint { return this.packet.sessionId; }
  get userId(): bigint { return this.packet.userId; }
  ensureSuccess(): Uint8Array | null {
    if (this.errorCode !== null) throw new GatewayRequestError(this.errorCode, this.errorMessage);
    return this.payload;
  }
}

export class KynetClient {
  private readonly config;
  private readonly endpoint: URL;
  private readonly codec: GatewayPacketCodec;
  private readonly factory: TransportFactory;
  private readonly listeners = new Map<keyof ClientEvents, Set<(value: never) => void>>();
  private readonly subscriptions = new Map<number, Set<(packet: GatewayPacket) => void>>();
  private lifetime?: AbortController;
  private ready?: Deferred<ClientSession>;
  private attempt?: Attempt;
  private nextId = 0n;
  private currentState: ClientState = "disconnected";
  private currentSession = session();

  constructor(options: ClientOptions = {}) {
    this.config = Object.freeze({ connectTimeoutMs: 5000, requestTimeoutMs: 3000,
      heartbeatIntervalMs: 5000, heartbeatTimeoutMs: 3000, recoveryTimeoutMs: 10000,
      autoReconnect: false, reconnectInitialDelayMs: 1000, reconnectMaxDelayMs: 15000,
      maxPendingRequests: 1024, maxPendingWrites: 256, ...options });
    this.endpoint = parseEndpoint(options.endpoint ?? "ws://127.0.0.1:30003/ws");
    this.codec = new GatewayPacketCodec(options);
    for (const key of ["connectTimeoutMs", "requestTimeoutMs", "heartbeatTimeoutMs", "recoveryTimeoutMs", "reconnectInitialDelayMs", "reconnectMaxDelayMs"] as const) {
      validateTimeout(this.config[key]);
    }
    validateTimeout(this.config.heartbeatIntervalMs, true);
    if (this.config.reconnectMaxDelayMs < this.config.reconnectInitialDelayMs) throw new RangeError("Invalid reconnect delay range.");
    for (const value of [this.config.maxPendingRequests, this.config.maxPendingWrites]) {
      if (!Number.isSafeInteger(value) || value < 1) throw new RangeError("Queue limits must be positive integers.");
    }
    this.factory = options.transportFactory ?? ((endpoint, codec) => new WebSocketTransport(endpoint, codec));
  }

  get state(): ClientState { return this.currentState; }
  get session(): ClientSession { return this.currentSession; }
  get pendingRequestCount(): number { return this.attempt?.pending.size ?? 0; }

  on<K extends keyof ClientEvents>(event: K, handler: (value: ClientEvents[K]) => void): () => void {
    let handlers = this.listeners.get(event);
    if (!handlers) this.listeners.set(event, handlers = new Set());
    const callback = handler as (value: never) => void;
    handlers.add(callback);
    return () => { if (handlers.delete(callback) && handlers.size === 0) this.listeners.delete(event); };
  }

  subscribe(cmd: number, act: number, handler: (packet: GatewayPacket) => void): () => void {
    const key = actionKey(cmd, act);
    let handlers = this.subscriptions.get(key);
    if (!handlers) this.subscriptions.set(key, handlers = new Set());
    handlers.add(handler);
    return () => { if (handlers.delete(handler) && handlers.size === 0) this.subscriptions.delete(key); };
  }

  subscribeMessage(messageId: number, handler: (packet: GatewayPacket) => void): () => void {
    return this.subscribe(...splitMessageId(messageId), handler);
  }

  async connect(signal?: AbortSignal): Promise<ClientSession> {
    if (signal?.aborted) throw aborted();
    if (!this.lifetime) {
      const lifetime = this.lifetime = new AbortController();
      this.ready = deferred<ClientSession>();
      void this.run(lifetime).catch(error => this.emit("error", asError(error)));
    }
    const ready = this.ready!;
    // Caller cancellation stops this wait; disconnect() ends the shared lifetime.
    return signal ? withDeadline(ready.promise, signal, 2147483647) : ready.promise;
  }

  disconnect(): void {
    const lifetime = this.lifetime;
    this.lifetime = undefined;
    const ready = this.ready;
    this.ready = undefined;
    const attempt = this.attempt;
    this.attempt = undefined;
    const stateChanged = this.currentState !== "disconnected";
    const sessionChanged = this.currentSession.sessionId !== 0n || this.currentSession.userId !== 0n;
    this.currentState = "disconnected";
    this.currentSession = session();
    lifetime?.abort();
    const error = new KynetError("DISCONNECTED", "Gateway client disconnected.");
    if (attempt) this.closeAttempt(attempt, error);
    ready?.reject(error);
    if (sessionChanged) this.emit("sessionChanged", this.currentSession);
    if (stateChanged && !this.lifetime) this.emit("stateChanged", "disconnected");
  }

  async request(cmd: number, act: number, payload: Uint8Array | null = null, options: RequestOptions = {}): Promise<ActionResponse> {
    validateAction(cmd, act);
    const response = await this.roundTrip(this.getAttempt(), packet({ cmd, act, payload }), PacketType.Response, options);
    return new ActionResponse(response);
  }

  requestMessage(messageId: number, payload: Uint8Array | null = null, options: RequestOptions = {}): Promise<ActionResponse> {
    return this.request(...splitMessageId(messageId), payload, options);
  }

  async sendOneWay(cmd: number, act: number, payload: Uint8Array | null = null, options: RequestOptions = {}): Promise<void> {
    validateAction(cmd, act);
    const attempt = this.getAttempt();
    const timeout = options.timeoutMs ?? this.config.requestTimeoutMs;
    validateTimeout(timeout);
    const stop = new AbortController();
    const onAbort = () => stop.abort();
    options.signal?.addEventListener("abort", onAbort, { once: true });
    if (options.signal?.aborted) stop.abort();
    try {
      await withDeadline(this.send(attempt, packet({ cmd, act, payload, oneWay: true, requestId: this.allocateId(), timeoutMs: BigInt(timeout) }), stop.signal), stop.signal, timeout);
    } finally { stop.abort(); options.signal?.removeEventListener("abort", onAbort); }
  }

  sendOneWayMessage(messageId: number, payload: Uint8Array | null = null, options: RequestOptions = {}): Promise<void> {
    return this.sendOneWay(...splitMessageId(messageId), payload, options);
  }

  async ping(options: RequestOptions = {}): Promise<ClientSession> {
    await this.roundTrip(this.getAttempt(), packet({ type: PacketType.Ping }), PacketType.Pong, {
      timeoutMs: this.config.heartbeatTimeoutMs, ...options });
    return this.session;
  }

  private async run(lifetime: AbortController): Promise<void> {
    let connectedOnce = false;
    let retryDelay = this.config.reconnectInitialDelayMs;
    while (this.lifetime === lifetime && !lifetime.signal.aborted) {
      let attempt: Attempt | undefined;
      let failure: Error = new KynetError("DISCONNECTED", "Gateway connection closed.");
      try {
        attempt = { transport: this.factory(new URL(this.endpoint), this.codec), stop: new AbortController(),
          closed: deferred<never>(), pending: new Map(), writeTail: Promise.resolve(), writes: 0 };
        this.attempt = attempt;
        this.setState(connectedOnce ? "reconnecting" : "connecting");
        if (this.lifetime !== lifetime) return;
        const current = attempt;
        await Promise.race([attempt.closed.promise, this.establish(attempt, lifetime, connectedOnce, () => {
          connectedOnce = true;
          retryDelay = this.config.reconnectInitialDelayMs;
        })]);
        await current.closed.promise;
      } catch (error) { failure = asError(error); }
      const retry = connectedOnce && this.config.autoReconnect && this.lifetime === lifetime && !lifetime.signal.aborted;
      if (attempt) this.closeAttempt(attempt, failure);
      if (this.lifetime !== lifetime) return;
      this.attempt = undefined;
      if (!retry) {
        this.lifetime = undefined;
        this.ready?.reject(failure);
      } else if (this.ready?.settled) this.ready = deferred<ClientSession>();
      // Commit state/session together before user callbacks can reconnect.
      const sessionChanged = this.currentSession.sessionId !== 0n || this.currentSession.userId !== 0n;
      const nextState = retry ? "reconnecting" : "disconnected";
      const stateChanged = this.currentState !== nextState;
      this.currentSession = session();
      this.currentState = nextState;
      if (sessionChanged) this.emit("sessionChanged", this.currentSession);
      if (this.lifetime && this.lifetime !== lifetime) return;
      if (stateChanged) this.emit("stateChanged", nextState);
      this.emit("error", failure);
      if (!retry || this.lifetime !== lifetime) return;
      try { await delay(retryDelay, lifetime.signal); } catch { return; }
      retryDelay = Math.min(retryDelay * 2, this.config.reconnectMaxDelayMs);
    }
  }

  private async establish(attempt: Attempt, lifetime: AbortController, reconnect: boolean, connected: () => void): Promise<void> {
    await withDeadline(attempt.transport.connect(attempt.stop.signal, {
      packet: data => {
        if (this.attempt !== attempt || attempt.stop.signal.aborted) return;
        try { this.receive(attempt, this.codec.decode(data)); }
        catch (error) { attempt.closed.reject(asError(error)); }
      },
      close: error => attempt.closed.reject(error),
    }), attempt.stop.signal, this.config.connectTimeoutMs, "CONNECT_TIMEOUT");
    await this.roundTrip(attempt, packet({ type: PacketType.Ping }), PacketType.Pong, { timeoutMs: this.config.connectTimeoutMs });
    if (this.attempt !== attempt || this.lifetime !== lifetime) throw aborted();
    if (this.session.sessionId <= 0n) throw new KynetError("HANDSHAKE_FAILED", "Gateway did not assign a session.");
    this.setState("recovering");
    if (this.attempt !== attempt) throw aborted();
    if (reconnect && this.config.sessionRecoveryHandler) {
      await withDeadline(Promise.resolve().then(() => this.config.sessionRecoveryHandler!(this, attempt.stop.signal)),
        attempt.stop.signal, this.config.recoveryTimeoutMs, "RECOVERY_TIMEOUT");
    }
    if (this.attempt !== attempt || attempt.stop.signal.aborted) throw aborted();
    connected();
    this.setState("connected");
    if (this.attempt !== attempt) throw aborted();
    this.ready?.resolve(this.session);
    // The heartbeat task is observed here so its failure closes the attempt.
    if (this.config.heartbeatIntervalMs === 0) return attempt.closed.promise;
    while (!attempt.stop.signal.aborted) {
      await delay(this.config.heartbeatIntervalMs, attempt.stop.signal);
      await this.roundTrip(attempt, packet({ type: PacketType.Ping }), PacketType.Pong, { timeoutMs: this.config.heartbeatTimeoutMs });
    }
  }

  private async roundTrip(attempt: Attempt, outgoing: GatewayPacket, expected: PacketType, options: RequestOptions): Promise<GatewayPacket> {
    if (options.signal?.aborted) throw aborted();
    const timeout = options.timeoutMs ?? this.config.requestTimeoutMs;
    validateTimeout(timeout);
    if (attempt.pending.size >= this.config.maxPendingRequests) throw new KynetError("QUEUE_FULL", "Maximum pending requests reached.");
    const id = this.allocateId();
    outgoing.requestId = id;
    outgoing.timeoutMs = BigInt(timeout);
    const completion = deferred<GatewayPacket>();
    const stop = new AbortController();
    const onAbort = () => stop.abort();
    options.signal?.addEventListener("abort", onAbort, { once: true });
    attempt.pending.set(id, { completion, type: expected, cmd: outgoing.cmd, act: outgoing.act });
    const exchange = Promise.all([this.send(attempt, outgoing, stop.signal), completion.promise])
      .then(([, response]) => response);
    try { return await withDeadline(exchange, stop.signal, timeout); }
    finally { attempt.pending.delete(id); stop.abort(); options.signal?.removeEventListener("abort", onAbort); }
  }

  private async send(attempt: Attempt, outgoing: GatewayPacket, signal: AbortSignal): Promise<void> {
    if (signal.aborted || attempt.stop.signal.aborted) throw aborted();
    if (this.attempt !== attempt) throw new KynetError("DISCONNECTED", "Connection changed before sending.");
    if (attempt.writes >= this.config.maxPendingWrites) throw new KynetError("QUEUE_FULL", "Maximum pending writes reached.");
    outgoing.sessionId = this.session.sessionId;
    outgoing.userId = this.session.userId;
    const data = this.codec.encode(outgoing);
    attempt.writes++;
    const write = attempt.writeTail.then(async () => {
      if (signal.aborted || attempt.stop.signal.aborted) throw aborted();
      if (this.attempt !== attempt) throw new KynetError("DISCONNECTED", "Connection changed before writing.");
      try { await attempt.transport.send(data, signal); }
      catch (error) { attempt.closed.reject(asError(error)); throw error; }
    });
    attempt.writeTail = write.catch(() => {});
    try { await write; } finally { attempt.writes--; }
  }

  private receive(attempt: Attempt, incoming: GatewayPacket): void {
    if (incoming.type === PacketType.Push) {
      for (const handler of [...(this.subscriptions.get(actionKey(incoming.cmd, incoming.act)) ?? [])]) {
        this.invoke(handler, incoming);
        if (this.attempt !== attempt) return;
      }
      this.emit("push", incoming);
      return;
    }
    if (incoming.type !== PacketType.Response && incoming.type !== PacketType.Pong) throw new KynetError("PROTOCOL_ERROR", "Unexpected inbound packet type.");
    if (this.session.sessionId > 0n && incoming.sessionId !== this.session.sessionId) throw new KynetError("SESSION_REPLACED", "Gateway replaced the session; login must be recovered.");
    const pending = attempt.pending.get(incoming.requestId);
    if (!pending) return;
    if (incoming.type !== pending.type || incoming.cmd !== pending.cmd || incoming.act !== pending.act) throw new KynetError("PROTOCOL_ERROR", "Response does not match pending request.");
    attempt.pending.delete(incoming.requestId);
    if (this.session.sessionId !== incoming.sessionId || this.session.userId !== incoming.userId) {
      this.currentSession = session(incoming.sessionId, incoming.userId);
      this.emit("sessionChanged", this.currentSession);
      if (this.attempt !== attempt) {
        pending.completion.reject(new KynetError("DISCONNECTED", "Connection changed during response callback."));
        return;
      }
    }
    pending.completion.resolve(incoming);
  }

  private closeAttempt(attempt: Attempt, error: Error): void {
    attempt.stop.abort();
    attempt.closed.reject(error);
    attempt.transport.close();
    for (const pending of attempt.pending.values()) pending.completion.reject(error);
    attempt.pending.clear();
  }

  private getAttempt(): Attempt {
    if (!this.attempt || this.attempt.stop.signal.aborted || !["connected", "recovering"].includes(this.state)) throw new KynetError("NOT_CONNECTED", "Gateway client is not ready.");
    return this.attempt;
  }
  private allocateId(): bigint { this.nextId = this.nextId === 0x7fffffffffffffffn ? 1n : this.nextId + 1n; return this.nextId; }
  private setState(state: ClientState): void { if (this.currentState !== state) { this.currentState = state; this.emit("stateChanged", state); } }
  private emit<K extends keyof ClientEvents>(event: K, value: ClientEvents[K]): void {
    for (const handler of [...(this.listeners.get(event) ?? [])]) {
      try { handler(value as never); } catch (error) { if (event !== "error") this.emit("error", asError(error)); }
    }
  }
  private invoke(handler: (packet: GatewayPacket) => void, incoming: GatewayPacket): void {
    try { handler(incoming); } catch (error) { this.emit("error", asError(error)); }
  }
}

function validateTimeout(value: number, allowZero = false): void {
  if (!Number.isInteger(value) || value < (allowZero ? 0 : 1) || value > 2147483647) throw new RangeError("Invalid timeout in milliseconds.");
}
function validateAction(cmd: number, act: number): void {
  actionKey(cmd, act);
  if (cmd === 0 || act === 0) throw new RangeError("Request cmd/act must be positive.");
}
