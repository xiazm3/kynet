# Kynet TypeScript 客户端 SDK

独立的 TypeScript SDK，连接本仓库 `GatewayServer`，与 C#、GDScript 客户端兼容同一 gateway wire v1。

- 浏览器：WebSocket / WSS，入口 `@kynet/client`，不引入 Node 套接字模块。
- Node.js：TCP / TLS、UDP、WebSocket / WSS，入口 `@kynet/client/node`。
- Promise 请求、业务错误码、AbortSignal 取消、超时、有界请求/发送队列、推送订阅。
- 首次 PING/PONG 会话握手、定时心跳、断线清理、可选指数退避重连及异步登录恢复。
- 请求 ID、会话 ID、用户 ID 使用 `bigint`，保留 Java signed int64 精度。
- 运行依赖 `protobufjs`、`long` 和 Node WebSocket 适配器的 `ws`；不依赖其他客户端目录。

## 构建与移植

复制整个 `kynet-client-typescript/` 目录即可作为独立项目构建，需要 Node.js 20.3+；原生 WebSocket 适配器的 Node 测试使用 Node.js 22.19+。

```powershell
npm ci
npm run build
npm test
npm pack
```

构建输出位于 `dist/`，包含 ESM JavaScript、`.d.ts`、source map。`npm pack` 生成可供业务项目安装的 `kynet-client-0.1.0.tgz`，无需发布 npm。业务项目也可使用本目录的 `file:` 依赖，先在 SDK 目录完成构建。若直接移植 `src/`，需同时安装 package.json 中的运行依赖，保持相对导入；TypeScript 的目标版本至少 ES2020，推荐 ES2022。

包采用 ESM，Node 项目使用 `import` 或动态 `import()`；浏览器工程使用 Vite、Webpack、esbuild 等打包工具。浏览器最低要求包括 BigInt、DataView 的 int64 方法、TextEncoder/TextDecoder、AbortController 和 WebSocket。

## Node.js 登录示例

```typescript
import { NodeKynetClient, loginMessage, stringValue, utf8 } from "@kynet/client/node";

const client = new NodeKynetClient({ endpoint: "tcp://127.0.0.1:30001" });
client.on("error", error => console.error(error.message));
const unsubscribe = client.subscribe(5, 6, packet => {
  if (packet.payload) console.log(stringValue.decode(packet.payload));
});

try {
  await client.connect();
  const response = await client.request(1, 1,
    loginMessage.encode({ username: "typescript", password: "123456" }));
  const payload = response.ensureSuccess();
  if (!payload) throw new Error("Missing login payload");
  console.log(loginMessage.decode(payload).token, client.session.userId);
  const hello = (await client.request(1, 2)).ensureSuccess();
  if (hello) console.log(utf8.decode(hello));
  (await client.request(1, 4)).ensureSuccess();
} finally {
  unsubscribe();
  client.disconnect();
}
```

先启动本仓库的注册中心、登录服务和网关，然后可执行独立示例：

```powershell
npm run example -- tcp://127.0.0.1:30001
```

账号 `typescript` / `123456` 来自仓库演示 LoginAction，仅用于开发测试。

## 浏览器

```typescript
import { KynetClient, loginMessage } from "@kynet/client";

const client = new KynetClient({ endpoint: "wss://game.example.com:443/ws" });
await client.connect();
const response = await client.request(1, 1,
  loginMessage.encode({ username: "player", password: "password" }));
if (!response.ok) console.error(response.errorCode, response.errorMessage);
```

浏览器默认入口只使用原生 WebSocket；HTTPS 页面应使用 WSS。浏览器证书信任由平台控制，不能设置私有 CA 或绕过证书校验。小游戏等非标准环境可通过 `transportFactory` 接入自己的传输实现，接口见 `src/transport.ts`。

## 请求、订阅与错误

| 接口 | 行为 |
| --- | --- |
| `connect(signal?)` | 等待握手及重连恢复，返回 ClientSession；重复调用共享连接 |
| `disconnect()` | 立即结束连接和重连，清空本地会话，拒绝所有待响应请求 |
| `request(cmd, act, payload?, options?)` | 返回 `Promise<ActionResponse>` |
| `requestMessage(messageId, payload?, options?)` | 按打包后的消息 ID 请求 |
| `sendOneWay(cmd, act, payload?, options?)` | 等待本地发送完成，不等待服务端响应 |
| `sendOneWayMessage(messageId, payload?, options?)` | 按消息 ID 单向发送 |
| `ping(options?)` | 等待 PONG，返回更新后的会话 |
| `subscribe(cmd, act, handler)` | 订阅推送，返回取消订阅函数 |
| `subscribeMessage(messageId, handler)` | 按消息 ID 订阅推送 |
| `on(event, handler)` | 注册事件，返回取消监听函数 |

`payload` 是 `Uint8Array | null`。默认 null，空字节数组与 null 严格区分。消息 ID 为 `(cmd << 16) | act`，支持 Java/C# 有符号 32 位值和对应的非负 32 位值。请求 cmd、act 范围为 1..65535。订阅回调接收完整 GatewayPacket，重复注册同一个函数不会重复回调。

ActionResponse 包含 `ok`、`payload`、`errorCode`、`errorMessage`、`sessionId`、`userId`、`packet`。服务端业务失败仍返回响应；`ensureSuccess()` 在失败时抛出 `GatewayRequestError`。空字符串 errorCode 也视为失败。网络、协议、超时、取消等错误拒绝 Promise；SDK 自身错误为 `KynetError`，可读取 `code`，如 `NOT_CONNECTED`、`TIMEOUT`、`CANCELLED`、`QUEUE_FULL`、`DISCONNECTED`、`PROTOCOL_ERROR`、`SESSION_REPLACED`。Node 传输也可能返回原生 Error，例如 TLS 证书校验错误。

```typescript
const abort = new AbortController();
const request = client.request(1, 2, null, { timeoutMs: 1500, signal: abort.signal });
// 在取消操作的业务回调中调用 abort.abort()。
try { await request; } catch (error) { console.error(error); }
```

取消 `connect(signal)` 只停止本次等待，`disconnect()` 才停止共享连接和重连。取消/超时的请求可能已经在服务端执行，不会撤销业务。发送中的 TCP/WS 写入被取消时会关闭传输，避免复用不完整的数据流。失败请求不会自动重放；交易、发奖等重试需要业务定义幂等键。

事件：`stateChanged`、`sessionChanged`、`push`、`error`。状态依次可为 `disconnected`、`connecting`、`recovering`、`connected`、`reconnecting`。订阅和事件回调同步执行，回调抛错会转发到 error 事件，不中断网络处理。异步回调中的 Promise 错误由业务自行处理。订阅跨断线保留。

## 重连与配置

```typescript
const client = new NodeKynetClient({
  endpoint: "tcp://127.0.0.1:30001",
  autoReconnect: true,
  sessionRecoveryHandler: async (gateway, signal) => {
    (await gateway.request(1, 1, loginMessage.encode({
      username: "typescript", password: "123456",
    }), { signal })).ensureSuccess();
  },
});
```

配置在构造时固定。首次连接失败直接拒绝 connect，不自动重试；首次成功后断线才启用自动重连。首次登录由业务执行；重连完成 PING/PONG 后进入 recovering，再调用恢复回调，完成后进入 connected。恢复回调失败或超时会继续退避重连；应遵守传入 signal，避免连接过期后继续执行业务。

| 配置 | 默认值 |
| --- | --- |
| `endpoint` | 浏览器 `ws://127.0.0.1:30003/ws`；Node `tcp://127.0.0.1:30001` |
| `connectTimeoutMs` | 5000，分别限制传输连接和首次 PING/PONG |
| `requestTimeoutMs` | 3000 |
| `heartbeatIntervalMs` / `heartbeatTimeoutMs` | 5000 / 3000，interval=0 关闭周期心跳，仍执行首次握手 |
| `recoveryTimeoutMs` | 10000 |
| `autoReconnect` | false |
| `reconnectInitialDelayMs` / `reconnectMaxDelayMs` | 1000 / 15000 |
| `maxPendingRequests` / `maxPendingWrites` | 1024 / 256 |
| `maxFrameLength` / `maxPayloadLength` / `maxStringLength` | 1 MiB / 512 KiB / 8 KiB |
| `transportFactory` | 浏览器 WebSocket；Node 按 scheme 选择传输 |
| `tls` | Node 专用，可设 ca、cert、key、servername；始终验证证书和主机名 |

所有 endpoint 都要求显式端口，IPv6 地址使用方括号。TCP/UDP 不接受路径或 query。TCP/TLS 使用 4 字节大端长度前缀，帧上限含此前缀；读取包体前检查长度。UDP/WS 一个包对应一个完整 wire 数据包。Node WS 限制接收最大消息大小，浏览器只在 WebSocket API 交付完整消息后检查长度，无法控制浏览器自身的接收缓存。

UDP 单包最多 65507 字节，不保证可靠性、顺序、去重或重发，建议显著减小业务包。顶号依靠关闭连接识别，UDP 通过心跳超时或 PONG 会话变化检测。wire v1 没有踢人原因字段；业务需要原因时应在踢人前发送通知，收到后可主动断开以停止自动重连。

`session` 的 sessionId、userId 是 bigint，isAuthenticated 根据 userId > 0n 判断。JSON 序列化时自行转字符串，不要转为 number，避免精度丢失。服务端 attachment 只在网关与服务内部流转，wire v1 不传输 attachment。

## Protobuf 载荷

SDK 用 protobufjs 的 Reader/Writer 提供示例载荷编解码，也兼容其他生成器输出的 Uint8Array。

| 服务端类型 | 编解码 |
| --- | --- |
| 普通 Action 的 String | `utf8.encode()` / `utf8.decode()` |
| 广播 / PlayerBroadcaster 的 String | Google StringValue，`stringValue.encode()` / `decode()` |
| 示例 LoginMessage | `loginMessage.encode()` / `decode()`，字段 1/2/3 对应 username/password/token |
| 普通 Action 的 int/long/bool/double/float | `primitiveValue.encode(kind, value)` / `decode(bytes, kind)`，kind 使用 PrimitiveKind |
| 任意业务 Protobuf | 用项目的生成器编码后传入 Uint8Array，对响应 payload 自行解码 |

PrimitiveKind.Long 使用 bigint；Java PrimitiveValue 与 Google Int64Value 等 wrapper 格式不同。消息号必须约定载荷类型，SDK 不从原始字节猜测类型。仓库示例 `1/5` 故意将 StringValue 和 LoginMessage 混合广播到 `200001`，业务需自行区分。

## 验证

`npm test` 包含协议黄金字节向量、截断和畸形包、64 位整数、Protobuf、队列上限、超时取消、请求关联、重复响应、回调重入、心跳、重连恢复、本机 TCP/UDP/WS/TLS/WSS 及证书信任/主机名验证。浏览器入口执行 esbuild 浏览器打包检查，并用 Node 22 原生 WebSocket 验证适配器。

真实 Java 网关联调需要 PowerShell 7、Java 25、Maven：

```powershell
./tests/interop/run-interop.ps1
# Java 模块已构建时：
./tests/interop/run-interop.ps1 -SkipJavaBuild
```

脚本复用 C# 客户端的 Java 测试服务，在临时端口启动真实 RegistryServer、LoginAction 和 Netty 网关，验证 Node TCP/UDP/WS 及原生 WebSocket 适配器的登录、鉴权、欢迎推送、attachment 连续性、广播、顶号和注销。结束时自动关闭测试进程；日志写入忽略提交的 artifacts/。可用 `-ClasspathFile` 复用已有 Java classpath 文件。

本地 TLS 测试使用公开自签名测试证书；生产证书链和真实浏览器部署仍应在目标环境验证。
