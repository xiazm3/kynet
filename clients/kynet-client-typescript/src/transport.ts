import { KynetError, aborted } from "./errors.js";
import type { GatewayPacketCodec } from "./protocol.js";

export interface TransportHandlers {
  packet(data: Uint8Array): void;
  close(error: Error): void;
}

export interface GatewayTransport {
  connect(signal: AbortSignal, handlers: TransportHandlers): Promise<void>;
  send(data: Uint8Array, signal: AbortSignal): Promise<void>;
  close(): void;
}

export type TransportFactory = (endpoint: URL, codec: GatewayPacketCodec) => GatewayTransport;

export function parseEndpoint(value: string): URL {
  const match = /^(tcp|tcps|udp|ws|wss):\/\/(\[[^\]]+\]|[^:/?#@\s]+):(\d+)([^#\s]*)$/.exec(value);
  if (!match || Number(match[3]) < 1 || Number(match[3]) > 65535) throw new TypeError("Use tcp/tcps/udp/ws/wss://host:port.");
  const url = new URL(value);
  if (url.username || url.password || url.hash) throw new TypeError("Invalid gateway endpoint.");
  if (["tcp:", "tcps:", "udp:"].includes(url.protocol) && ((url.pathname && url.pathname !== "/") || url.search)) {
    throw new TypeError("TCP/UDP endpoints cannot contain a path or query.");
  }
  return url;
}

/** Abort/deadline wrapper always observes the underlying promise and removes listeners. */
export function withDeadline<T>(promise: Promise<T>, signal: AbortSignal, timeoutMs: number, code = "TIMEOUT"): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      callback();
    };
    const onAbort = () => finish(() => reject(aborted()));
    const timer = setTimeout(() => finish(() => reject(new KynetError(code, "Gateway operation timed out."))), timeoutMs);
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(value => finish(() => resolve(value)), error => finish(() => reject(error)));
    if (signal.aborted) onAbort();
  });
}

export function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => { clearTimeout(timer); signal.removeEventListener("abort", onAbort); };
    const onAbort = () => { cleanup(); reject(aborted()); };
    const timer = setTimeout(() => { cleanup(); resolve(); }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
}

/** Browser WebSocket adapter. No Node built-ins are imported by the root entry. */
export class WebSocketTransport implements GatewayTransport {
  private socket?: WebSocket;
  private stopped = false;
  private cancelConnect?: () => void;

  constructor(private readonly endpoint: URL, private readonly codec: GatewayPacketCodec) {}

  connect(signal: AbortSignal, handlers: TransportHandlers): Promise<void> {
    if (!["ws:", "wss:"].includes(this.endpoint.protocol)) return Promise.reject(new TypeError("Browser transport requires ws/wss."));
    return new Promise((resolve, reject) => {
      if (signal.aborted) { reject(aborted()); return; }
      const socket = this.socket = new WebSocket(this.endpoint);
      socket.binaryType = "arraybuffer";
      let opened = false;
      const stopWaiting = () => { signal.removeEventListener("abort", onAbort); this.cancelConnect = undefined; };
      const fail = (error: Error) => {
        if (this.stopped) return;
        if (!opened) { stopWaiting(); reject(error); }
        handlers.close(error);
        this.close();
      };
      const onAbort = () => { fail(aborted()); };
      this.cancelConnect = () => { stopWaiting(); reject(new KynetError("DISCONNECTED", "WebSocket closed.")); };
      signal.addEventListener("abort", onAbort, { once: true });
      socket.onopen = () => { opened = true; stopWaiting(); resolve(); };
      socket.onerror = () => fail(new KynetError("CONNECTION_LOST", "WebSocket connection failed."));
      socket.onclose = () => fail(new KynetError("DISCONNECTED", "Gateway WebSocket closed."));
      socket.onmessage = event => {
        try {
          if (!(event.data instanceof ArrayBuffer)) throw new KynetError("PROTOCOL_ERROR", "Expected a binary WebSocket message.");
          const data = new Uint8Array(event.data);
          this.codec.validateLength(data.length);
          handlers.packet(data);
        } catch (error) { fail(error instanceof Error ? error : new Error(String(error))); }
      };
    });
  }

  async send(data: Uint8Array, signal: AbortSignal): Promise<void> {
    if (signal.aborted) throw aborted();
    const socket = this.socket;
    if (this.stopped || !socket || socket.readyState !== WebSocket.OPEN) throw new KynetError("DISCONNECTED", "WebSocket is not open.");
    // Browser APIs expose bufferedAmount rather than a write-completion callback.
    while (socket.bufferedAmount + data.length > this.codec.limits.maxFrameLength) {
      await delay(5, signal);
      if (this.stopped || socket.readyState !== WebSocket.OPEN) throw new KynetError("DISCONNECTED", "WebSocket closed while writing.");
    }
    socket.send(data);
  }

  close(): void {
    this.stopped = true;
    this.cancelConnect?.();
    if (this.socket) {
      this.socket.onopen = this.socket.onmessage = this.socket.onclose = this.socket.onerror = null;
      this.socket.close();
      this.socket = undefined;
    }
  }
}
