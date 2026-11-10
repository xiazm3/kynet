import test from "node:test";
import assert from "node:assert/strict";
import { build } from "esbuild";
import { GatewayPacketCodec, packet, PacketType, actionKey, splitMessageId, parseEndpoint,
  loginMessage, stringValue, primitiveValue, PrimitiveKind, utf8, ActionResponse } from "../dist/index.js";
import { TcpFrameDecoder } from "../dist/node-transport.js";

const codec = new GatewayPacketCodec();
const golden = packet({ requestId: 0x0102030405060708n, cmd: 1, act: 2, timeoutMs: 3000n,
  sessionId: 0x7fffffffffffffffn, userId: 10001n, payload: Uint8Array.of(10, 1, 97) });

test("Java/C# golden wire bytes, exact int64 and Uint8Array offsets", () => {
  const data = codec.encode(golden);
  assert.equal(Buffer.from(data).toString("hex"), "4b594e5400010000010203040506070800000001000000020000000000000bb8" +
    "7fffffffffffffff0000000000002711ffffffffffffffff000000030a0161");
  assert.deepEqual(codec.decode(Buffer.concat([Buffer.alloc(7), data]).subarray(7)), golden);
});

test("null and empty fields are distinct, including empty business errors", () => {
  assert.equal(codec.decode(codec.encode(packet())).payload, null);
  const decoded = codec.decode(codec.encode(packet({ payload: new Uint8Array(), errorCode: "" })));
  assert.deepEqual(decoded.payload, new Uint8Array());
  assert.equal(new ActionResponse(decoded).ok, false);
  assert.throws(() => new ActionResponse(decoded).ensureSuccess());
});

test("reject all truncations, trailing data, malformed lengths, headers and UTF-8", () => {
  const data = codec.encode(golden);
  for (let length = 0; length < data.length; length++) assert.throws(() => codec.decode(data.subarray(0, length)));
  assert.throws(() => codec.decode(Buffer.concat([data, Buffer.alloc(1)])));
  for (const offset of [0, 5, 6, 48]) {
    const bad = data.slice(); bad[offset] = 127;
    assert.throws(() => codec.decode(bad));
  }
  const bad = codec.encode(packet({ errorMessage: "a" }));
  bad[56] = 255;
  assert.throws(() => codec.decode(bad));
  const bom = "\ufefferror";
  assert.equal(codec.decode(codec.encode(packet({ errorCode: bom }))).errorCode, bom);
});

test("enforce frame, payload, string byte and int64 limits", () => {
  const small = new GatewayPacketCodec({ maxFrameLength: 128, maxPayloadLength: 2, maxStringLength: 3 });
  assert.throws(() => small.encode(golden));
  assert.throws(() => small.decode(codec.encode(golden)));
  assert.throws(() => small.encode(packet({ errorCode: "中文" })));
  assert.throws(() => codec.encode(packet({ sessionId: 2n ** 63n })));
  assert.throws(() => codec.encode(packet({ requestId: 1 })));
});

test("TCP framing accepts fragmentation and coalescing and rejects prefix before allocation", () => {
  const raw = codec.encode(golden);
  const frame = Buffer.alloc(raw.length + 4); frame.writeInt32BE(raw.length); frame.set(raw, 4);
  const decoder = new TcpFrameDecoder(codec);
  const packets = [];
  for (const byte of frame) decoder.feed(Uint8Array.of(byte), data => packets.push(data));
  decoder.feed(Buffer.concat([frame, frame]), data => packets.push(data));
  assert.equal(packets.length, 3);
  for (const data of packets) assert.deepEqual(data, raw);
  assert.throws(() => new TcpFrameDecoder(codec).feed(Uint8Array.of(127, 255, 255, 255), () => {}));
});

test("action keys and endpoint validation", () => {
  assert.equal(actionKey(65535, 65535), 0xffffffff);
  assert.deepEqual(splitMessageId(-1), [65535, 65535]);
  assert.throws(() => actionKey(65536, 1));
  assert.throws(() => splitMessageId(2 ** 32));
  assert.equal(parseEndpoint("tcp://[::1]:30001").hostname, "[::1]");
  assert.equal(parseEndpoint("wss://host:443/ws?x=1").pathname, "/ws");
  for (const endpoint of ["tcp://host", "tcp://host:0", "tcp://host:65536", "tcp://host:1/x", "http://host:1", "ws://a:b@host:1", "ws://host:1/#x"]) {
    assert.throws(() => parseEndpoint(endpoint), endpoint);
  }
});

test("Protobuf login, Google wrappers and Java PrimitiveValue golden bytes", () => {
  assert.equal(Buffer.from(loginMessage.encode({ username: "a", password: "b", token: "c" })).toString("hex"), "0a01611201621a0163");
  assert.equal(stringValue.decode(Buffer.from("0a0777656c636f6d65", "hex")), "welcome");
  assert.equal(Buffer.from(primitiveValue.encode(PrimitiveKind.Int, -1)).toString("hex"), "080010ffffffffffffffffff01");
  for (const [kind, value] of [[0, 0], [0, -2147483648], [1, -(2n ** 63n)], [1, 2n ** 63n - 1n], [2, false], [2, true], [3, 1.25], [4, -2.5]]) {
    assert.equal(primitiveValue.decode(primitiveValue.encode(kind, value), kind), value);
  }
  assert.throws(() => primitiveValue.decode(primitiveValue.encode(1, 42n), 0));
  assert.throws(() => stringValue.decode(Uint8Array.of(10, 255)));
  assert.throws(() => stringValue.decode(Uint8Array.of(0)));
  assert.equal(utf8.decode(utf8.encode("中文")), "中文");
  assert.throws(() => utf8.decode(Uint8Array.of(255)));
});

test("browser entry bundles without Node built-ins", async () => {
  const result = await build({ entryPoints: ["src/index.ts"], bundle: true, write: false,
    platform: "browser", format: "esm", target: "es2022", metafile: true });
  assert.equal(Object.keys(result.metafile.inputs).some(name => name.includes("node-transport") || name.includes("node_modules/ws/")), false);
  assert.ok(result.outputFiles[0].contents.length > 0);
});
