import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { KynetClient, GatewayPacketCodec, PacketType, packet } from "../dist/index.js";

class FakeTransport {
  codec = new GatewayPacketCodec();
  sent = [];
  open = false;
  sessionId = 0x7ffffffffffffffen;
  pong = true;
  mismatch = false;
  holdWrites = false;
  duplicate = false;
  constructor(index) { this.sessionId -= BigInt(index); }
  async connect(signal, handlers) { this.handlers = handlers; if (signal.aborted) throw new Error("aborted"); this.open = true; }
  async send(data, signal) {
    const request = this.codec.decode(data); this.sent.push(request);
    if (this.holdWrites) await new Promise((_, reject) => signal.addEventListener("abort", () => reject(new Error("aborted write")), { once: true }));
    if (request.oneWay || (request.type === PacketType.Ping && !this.pong) || request.act === 99) return;
    await Promise.resolve();
    this.push(packet({ type: request.type === PacketType.Ping ? PacketType.Pong : PacketType.Response,
      requestId: request.requestId, cmd: request.cmd, act: request.act + (this.mismatch ? 1 : 0),
      sessionId: this.sessionId, payload: request.payload, userId: request.act === 1 ? 10001n : 0n,
      errorCode: request.act === 9 ? "BUSINESS_ERROR" : null }));
    if (this.duplicate) this.push(packet({ type: PacketType.Response, requestId: request.requestId,
      cmd: request.cmd, act: request.act, sessionId: this.sessionId, userId: 99999n }));
  }
  push(value) { if (this.open) this.handlers.packet(this.codec.encode(value)); }
  drop() { this.open = false; this.handlers.close(new Error("dropped")); }
  close() { this.open = false; }
}

function setup(options = {}) {
  const transports = [];
  const client = new KynetClient({ endpoint: "tcp://localhost:30001", heartbeatIntervalMs: 0,
    requestTimeoutMs: 200, connectTimeoutMs: 200, reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 20,
    transportFactory: () => { const transport = new FakeTransport(transports.length); transports.push(transport); return transport; }, ...options });
  return { client, transports };
}
async function until(condition) {
  const end = Date.now() + 1000;
  while (!condition() && Date.now() < end) await sleep(5);
  assert.ok(condition());
}

test("request correlation, exact session, business errors, messageId and one-way", async t => {
  const { client, transports } = setup(); t.after(() => client.disconnect());
  await assert.rejects(client.request(1, 2), { code: "NOT_CONNECTED" });
  await client.connect();
  assert.equal(client.session.sessionId, transports[0].sessionId);
  const [first, second] = await Promise.all([client.request(1, 1), client.requestMessage(65538)]);
  assert.ok(first.ok && second.ok);
  const failed = await client.request(1, 9);
  assert.equal(failed.errorCode, "BUSINESS_ERROR");
  assert.throws(() => failed.ensureSuccess(), { code: "BUSINESS_ERROR" });
  await client.sendOneWayMessage(65539);
  assert.ok(transports[0].sent.at(-1).oneWay);
  assert.equal(client.pendingRequestCount, 0);
});

test("push subscriptions survive reconnect, unsubscribe and callback isolation", async t => {
  const { client, transports } = setup({ autoReconnect: true }); t.after(() => client.disconnect());
  let pushes = 0, errors = 0;
  client.on("error", () => { errors++; });
  client.subscribeMessage(200001, () => { throw new Error("listener failure"); });
  const unsubscribe = client.subscribeMessage(200001, () => { pushes++; });
  await client.connect();
  transports[0].push(packet({ type: PacketType.Push, cmd: 3, act: 3393 }));
  assert.equal(pushes, 1); assert.equal(errors, 1);
  transports[0].drop();
  await until(() => transports.length > 1 && client.state === "connected");
  transports[1].push(packet({ type: PacketType.Push, cmd: 3, act: 3393 }));
  assert.equal(pushes, 2);
  unsubscribe();
  transports[1].push(packet({ type: PacketType.Push, cmd: 3, act: 3393 }));
  assert.equal(pushes, 2);
});

test("request timeout, AbortSignal, queue limits and disconnect complete pending promises", async t => {
  const { client } = setup({ maxPendingRequests: 1 }); t.after(() => client.disconnect());
  await client.connect();
  const pending = client.request(1, 99, null, { timeoutMs: 30 });
  await assert.rejects(client.request(1, 2), { code: "QUEUE_FULL" });
  await assert.rejects(pending, { code: "TIMEOUT" });
  const stop = new AbortController();
  const cancelled = client.request(1, 99, null, { signal: stop.signal }); stop.abort();
  await assert.rejects(cancelled, { code: "CANCELLED" });
  const outstanding = client.request(1, 99); client.disconnect();
  await assert.rejects(outstanding, { code: "DISCONNECTED" });
  assert.equal(client.session.sessionId, 0n); assert.equal(client.pendingRequestCount, 0);
});

test("connect caller cancellation leaves shared connection running", async t => {
  const { client } = setup(); t.after(() => client.disconnect());
  const stop = new AbortController();
  const connection = client.connect(stop.signal); stop.abort();
  await assert.rejects(connection, { code: "CANCELLED" });
  await client.connect(); assert.equal(client.state, "connected");
});

test("response mismatch and replaced session fail pending requests", async t => {
  const { client, transports } = setup(); t.after(() => client.disconnect());
  await client.connect(); transports[0].mismatch = true;
  await assert.rejects(client.request(1, 2), { code: "PROTOCOL_ERROR" });
  assert.equal(client.state, "disconnected");
  await client.connect(); transports[1].sessionId++;
  await assert.rejects(client.request(1, 2), { code: "SESSION_REPLACED" });
});

test("reconnect awaits login recovery, never replays requests, manual disconnect stops retries", async t => {
  let recoveries = 0;
  const { client, transports } = setup({ autoReconnect: true, sessionRecoveryHandler: async gateway => {
    recoveries++; (await gateway.request(1, 1)).ensureSuccess();
  } }); t.after(() => client.disconnect());
  await client.connect(); assert.equal(recoveries, 0);
  const pending = client.request(1, 99); transports[0].drop();
  await assert.rejects(pending);
  await until(() => transports.length === 2 && client.state === "connected");
  assert.equal(recoveries, 1); assert.equal(client.session.userId, 10001n);
  assert.equal(transports[1].sent.some(request => request.act === 99), false);
  client.disconnect(); await sleep(30); assert.equal(transports.length, 2);
});

test("heartbeat detects lost PONG and recovery has a deadline", async t => {
  const { client, transports } = setup({ heartbeatIntervalMs: 5, heartbeatTimeoutMs: 15 }); t.after(() => client.disconnect());
  await client.connect(); transports[0].pong = false;
  await until(() => client.state === "disconnected"); assert.equal(client.session.sessionId, 0n);
  const recovery = setup({ autoReconnect: true, recoveryTimeoutMs: 15,
    sessionRecoveryHandler: () => new Promise(() => {}) });
  t.after(() => recovery.client.disconnect());
  const errors = [];
  recovery.client.on("error", error => { errors.push(error.code); if (error.code === "RECOVERY_TIMEOUT") recovery.client.disconnect(); });
  await recovery.client.connect(); recovery.transports[0].drop();
  await until(() => errors.includes("RECOVERY_TIMEOUT"));
});

test("send queue limit and cancellation during active send close broken transport", async t => {
  const { client, transports } = setup({ maxPendingWrites: 1 }); t.after(() => client.disconnect());
  await client.connect(); transports[0].holdWrites = true;
  const pending = client.request(1, 2, null, { timeoutMs: 20 });
  await assert.rejects(client.request(1, 2), { code: "QUEUE_FULL" });
  await assert.rejects(pending, { code: "TIMEOUT" });
  await until(() => client.state === "disconnected");
});

test("disconnect callback can immediately reconnect without old cleanup overwriting state", async t => {
  const { client } = setup(); t.after(() => client.disconnect());
  await client.connect();
  let restart;
  const off = client.on("stateChanged", state => { if (state === "disconnected") { off(); restart = client.connect(); } });
  client.disconnect(); await restart; assert.equal(client.state, "connected");
});

test("duplicate responses cannot overwrite session metadata", async t => {
  const { client, transports } = setup(); t.after(() => client.disconnect());
  await client.connect(); transports[0].duplicate = true;
  await client.request(1, 1);
  assert.equal(client.session.userId, 10001n);
});

test("disconnect in response session callback completes the removed pending request", async t => {
  const { client } = setup(); t.after(() => client.disconnect());
  await client.connect();
  client.on("sessionChanged", session => { if (session.userId === 10001n) client.disconnect(); });
  await assert.rejects(client.request(1, 1), { code: "DISCONNECTED" });
});

test("unsubscribe handles are idempotent after a new subscription is registered", async t => {
  const { client, transports } = setup(); t.after(() => client.disconnect());
  let pushes = 0, events = 0;
  const oldPush = client.subscribe(1, 2, () => {}), oldEvent = client.on("push", () => {});
  oldPush(); oldEvent();
  client.subscribe(1, 2, () => { pushes++; }); client.on("push", () => { events++; });
  oldPush(); oldEvent();
  await client.connect();
  transports[0].push(packet({ type: PacketType.Push, cmd: 1, act: 2 }));
  assert.equal(pushes, 1); assert.equal(events, 1);
});
