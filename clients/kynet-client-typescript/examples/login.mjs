import { NodeKynetClient, loginMessage, stringValue, utf8 } from "../dist/node.js";

const client = new NodeKynetClient({ endpoint: process.argv[2] ?? "tcp://127.0.0.1:30001" });
client.on("error", error => console.error(error.message));
const unsubscribe = client.subscribe(5, 6, message => console.log(stringValue.decode(message.payload)));
try {
  await client.connect();
  const response = await client.request(1, 1, loginMessage.encode({ username: "typescript", password: "123456" }));
  const login = loginMessage.decode(response.ensureSuccess());
  console.log("userId=", client.session.userId.toString(), "token=", login.token);
  console.log(utf8.decode((await client.request(1, 2)).ensureSuccess()));
  (await client.request(1, 4)).ensureSuccess();
} finally {
  unsubscribe();
  client.disconnect();
}
