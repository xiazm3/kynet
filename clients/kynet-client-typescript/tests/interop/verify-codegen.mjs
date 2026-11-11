// 端到端冒烟：用 kynet-codegen 生成的 SignAction（TS）调真实服务器。
import { NodeKynetClient } from "../../dist/node.js";
import { SignAction } from "../../dist/generated/sign-action.js";

const endpoint = process.argv[2];
if (!endpoint) throw new Error("usage: node verify-codegen.mjs tcp://host:port");

const client = new NodeKynetClient({ endpoint,
  heartbeatIntervalMs: 250, heartbeatTimeoutMs: 2000 });
await client.connect();

const check = (condition, name) => {
  if (!condition) throw new Error("smoke failed: " + name);
};

const signResponse = await SignAction.sign(client, { ticket: "abc" });
signResponse.log();
check(signResponse.ok, "sign ok");
check(signResponse.data.day === 3, "sign day");
check(signResponse.data.bonus === "签到成功：abc", "sign bonus");
check(signResponse.ensureSuccess().day === 3, "ensureSuccess");

check((await SignAction.nextDay(client, 2)).ensureSuccess() === 3, "nextDay");

const rewards = (await SignAction.rewards(client, "kynet")).ensureSuccess();
check(rewards.length === 2 && rewards[0] === "kynet-首签礼包" && rewards[1] === "连续签到+500金币",
  "rewards values");

await SignAction.nextDay(client, 9).then((response) => {
  check(response.ensureSuccess() === 10, "Promise then nextDay");
});

console.log("CODEGEN TS SMOKE PASS", endpoint);
client.disconnect();
