import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import tls from "node:tls";
import http from "node:http";
import https from "node:https";
import dgram from "node:dgram";
import { readFile } from "node:fs/promises";
import { once } from "node:events";
import { WebSocketServer } from "ws";
import { NodeKynetClient, KynetClient, GatewayPacketCodec, PacketType, packet, utf8 } from "../dist/node.js";
import { TcpFrameDecoder } from "../dist/node-transport.js";

const codec = new GatewayPacketCodec();
const key = await readFile(new URL("fixtures/server-key.pem", import.meta.url));
const cert = await readFile(new URL("fixtures/server-cert.pem", import.meta.url));

async function fixture(protocol) {
  let session = 10n;
  const peers = new Set();
  const response = (data, id) => {
    const input = codec.decode(data);
    return codec.encode(packet({ type: input.type === PacketType.Ping ? PacketType.Pong : PacketType.Response,
      requestId: input.requestId, cmd: input.cmd, act: input.act, sessionId: id, payload: input.payload }));
  };
  const secure = protocol === "tcps" || protocol === "wss";
  let server, websocket;
  if (protocol === "tcp" || protocol === "tcps") {
    const handle = socket => {
      peers.add(socket); socket.on("close", () => peers.delete(socket)); socket.on("error", () => {});
      const id = ++session;
      const decoder = new TcpFrameDecoder(codec);
      let writes = Promise.resolve();
      socket.on("data", data => decoder.feed(data, raw => {
        const result = response(raw, id);
        const frame = Buffer.alloc(result.length + 4); frame.writeInt32BE(result.length); frame.set(result, 4);
        writes = writes.then(() => new Promise(resolve => {
          if (socket.destroyed) { resolve(); return; }
          socket.write(frame.subarray(0, 2));
          setImmediate(() => { if (!socket.destroyed) socket.write(frame.subarray(2)); resolve(); });
        }));
      }));
    };
    server = secure ? tls.createServer({ key, cert }, handle) : net.createServer(handle);
    server.listen(0, "127.0.0.1"); await once(server, "listening");
  } else if (protocol === "udp") {
    server = dgram.createSocket("udp4");
    server.on("message", (data, remote) => server.send(response(data, 42n), remote.port, remote.address));
    server.bind(0, "127.0.0.1"); await once(server, "listening");
  } else {
    server = secure ? https.createServer({ key, cert }) : http.createServer();
    websocket = new WebSocketServer({ server });
    websocket.on("connection", socket => {
      peers.add(socket); socket.on("close", () => peers.delete(socket));
      const id = ++session;
      socket.on("message", data => socket.send(response(data, id)));
    });
    server.listen(0, "127.0.0.1"); await once(server, "listening");
  }
  return {
    endpoint: `${protocol}://127.0.0.1:${server.address().port}${protocol.endsWith("s") && protocol !== "tcps" || protocol === "ws" ? "/ws" : ""}`,
    close: async () => {
      for (const peer of peers) { if (peer.terminate) peer.terminate(); else peer.destroy(); }
      websocket?.close();
      await new Promise(resolve => server.close(resolve));
    },
  };
}

for (const protocol of ["tcp", "udp", "ws", "tcps", "wss"]) {
  test(`Node ${protocol}: real socket handshake and request/response`, { timeout: 5000 }, async t => {
    const server = await fixture(protocol); t.after(server.close);
    const client = new NodeKynetClient({ endpoint: server.endpoint, heartbeatIntervalMs: 0,
      ...(protocol === "tcps" || protocol === "wss" ? { tls: { ca: cert } } : {}) });
    t.after(() => client.disconnect());
    await client.connect();
    const responses = await Promise.all([client.request(1, 2, utf8.encode("socket")), client.request(1, 3, new Uint8Array())]);
    assert.equal(utf8.decode(responses[0].ensureSuccess()), "socket");
    assert.equal(responses[1].payload.length, 0);
    assert.equal(client.session.sessionId > 0n, true);
  });
}

test("browser WebSocket adapter uses native WebSocket with real server", { timeout: 5000 }, async t => {
  const server = await fixture("ws"); t.after(server.close);
  const client = new KynetClient({ endpoint: server.endpoint, heartbeatIntervalMs: 0 }); t.after(() => client.disconnect());
  await client.connect();
  assert.equal(utf8.decode((await client.request(1, 2, utf8.encode("browser"))).ensureSuccess()), "browser");
});

for (const protocol of ["tcps", "wss"]) {
  test(`${protocol}: rejects an untrusted certificate`, { timeout: 5000 }, async t => {
    const server = await fixture(protocol); t.after(server.close);
    const client = new NodeKynetClient({ endpoint: server.endpoint, connectTimeoutMs: 500 }); t.after(() => client.disconnect());
    await assert.rejects(client.connect(), /self-signed certificate/i);
    assert.equal(client.state, "disconnected");
    assert.equal(client.session.sessionId, 0n);
  });
  test(`${protocol}: validates certificate hostname with a trusted CA`, { timeout: 5000 }, async t => {
    const server = await fixture(protocol); t.after(server.close);
    const client = new NodeKynetClient({ endpoint: server.endpoint, tls: { ca: cert, servername: "wrong.example" } });
    t.after(() => client.disconnect());
    await assert.rejects(client.connect(), /hostname|altnames/i);
  });
}
