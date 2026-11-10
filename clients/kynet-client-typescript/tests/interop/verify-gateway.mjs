import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { NodeKynetClient, KynetClient, loginMessage, stringValue, utf8 } from "../../dist/node.js";

const endpoint = process.argv[2];
const Client = process.argv.includes("--browser") ? KynetClient : NodeKynetClient;
const create = () => new Client({ endpoint, heartbeatIntervalMs: 100, heartbeatTimeoutMs: 1000 });
const client = create(), second = create();
const deadline = setTimeout(() => { console.error("Interop deadline exceeded."); process.exit(2); }, 30000);
let welcome = "", broadcasts = 0, checks = 0;
function check(value, message) { assert.ok(value, message); checks++; }
async function until(condition) {
  const end = Date.now() + 4000;
  while (!condition() && Date.now() < end) await delay(10);
  return condition();
}
client.subscribe(5, 6, message => { welcome = stringValue.decode(message.payload); });
client.subscribeMessage(200001, () => { broadcasts++; });
try {
  await client.connect();
  check(client.session.sessionId > 0n, "session handshake");
  check((await client.request(1, 2)).errorCode === "UNAUTHORIZED", "protected action requires login");
  check((await client.request(1, 1, loginMessage.encode({ username: "typescript", password: "invalid" }))).errorCode === "LOGIN_FAILED", "login business error");
  const credentials = loginMessage.encode({ username: "typescript", password: "123456" });
  const login = loginMessage.decode((await client.request(1, 1, credentials)).ensureSuccess());
  check(client.session.userId === 10001n, "login updates user");
  check(login.token === "token-typescript", "Protobuf login response");
  check(await until(() => welcome === "welcome typescript"), "Google StringValue welcome");
  check(utf8.decode((await client.request(1, 2)).ensureSuccess()).includes("attachment.username=typescript"), "attachment continuity");
  (await client.request(1, 5)).ensureSuccess();
  check(await until(() => broadcasts >= 6), "six broadcasts");
  await client.sendOneWay(1, 3, credentials); checks++;
  await second.connect(); check(second.session.sessionId > 0n, "second session");
  check((await second.request(1, 1, credentials)).ok, "second login");
  check(await until(() => client.state === "disconnected"), "kick closes old connection");
  check(client.session.sessionId === 0n && client.session.userId === 0n, "kick clears session");
  check((await second.request(1, 4)).ok, "logout");
  check((await second.request(1, 2)).errorCode === "UNAUTHORIZED" && second.session.userId === 0n, "logout clears authentication");
  console.log(`INTEROP PASS ${endpoint} (${checks} checks, ${Client.name})`);
} finally {
  clearTimeout(deadline); client.disconnect(); second.disconnect();
}
