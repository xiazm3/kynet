# kynet C# 玩家客户端

连接当前仓库的 `GatewayServer`，目标框架为 `.NET Standard 2.0`，可供支持该标准的 Mono、Unity 和 .NET 项目引用。类库不依赖 Unity 或 DotNetty；控制台示例和测试使用 .NET 8。

## 功能

- `tcp://`、`tcps://`、`udp://`、`ws://`、`wss://`；TLS 使用系统证书信任和主机名校验。
- 按 `cmd/act` 或打包后的 `messageId` 发起异步请求、发送单向消息、订阅推送和广播。
- 请求关联、业务错误码、超时、取消、有界请求/发送队列、断线时清理所有待响应请求。
- 首次连接执行 PING/PONG，获取服务端会话；定时心跳检测失联。
- 可选指数退避重连与重新登录回调；每次断线清除本地会话信息。
- 支持 Google.Protobuf 的生成类和 protobuf-net 的 `[ProtoContract]` 类，以及原始字节、字符串和 Java `PrimitiveValue` 基础类型。
- 可指定 `SynchronizationContext`，把状态、推送、会话及错误回调投递到游戏主线程。

## 构建与运行

在本目录执行：

```powershell
dotnet restore Kynet.Client.sln --configfile NuGet.Config
dotnet build Kynet.Client.sln -c Release --no-restore
dotnet test Kynet.Client.sln -c Release --no-build --no-restore
dotnet run --project samples/Kynet.Client.Console -- tcp://127.0.0.1:30001
```

示例需要先启动仓库的注册中心、登录服务和网关。它会验证免登录访问被拒绝、错误密码、登录、欢迎推送、hello、广播和注销。示例账号为 `mono`，密码为 `123456`，来自仓库的 `LoginAction`。

产物：`src/Kynet.Client/bin/Release/netstandard2.0/Kynet.Client.dll`。业务工程可直接添加 `ProjectReference`，或执行 `dotnet pack src/Kynet.Client -c Release` 生成 NuGet 包。Unity 导入 DLL 时也要导入 NuGet 依赖及其传递依赖，避免与项目中已有的 Protobuf DLL 重复。

## 接入示例

```csharp
using Google.Protobuf.WellKnownTypes;
using Kynet.Client;
using ProtoBuf;

[ProtoContract]
public sealed class LoginMessage
{
    [ProtoMember(1)] public string Username { get; set; }
    [ProtoMember(2)] public string Password { get; set; }
    [ProtoMember(3)] public string Token { get; set; }
}

var client = new NetClient(new NetClientOptions
{
    Endpoint = "tcp://127.0.0.1:30001",
    CallbackContext = System.Threading.SynchronizationContext.Current
});
client.Error += exception => Log(exception.Message);
var subscription = client.Subscribe<StringValue>(5, 6, msg => Log(msg.Value));
await client.ConnectAsync();

var response = await client.RequestAsync<LoginMessage>(1, 1,
    new LoginMessage { Username = "mono", Password = "123456" });
LoginMessage login = response.EnsureSuccess();
long userId = client.Session.UserId;
string hello = (await client.RequestAsync<string>(1, 2)).EnsureSuccess();

// messageId = (cmd << 16) | act，也可使用 Subscribe<T>(messageId, handler)。
var rawSubscription = client.Subscribe(200001, packet => HandleBytes(packet.Payload));
await client.SendOneWayAsync(1, 3, login);

subscription.Dispose();
rawSubscription.Dispose();
await client.DisconnectAsync();
client.Dispose();
```

`Log`、`HandleBytes` 替换为业务实现。`ActionResponse<T>` 的 `IsSuccess`、`ErrorCode`、`ErrorMessage` 保留服务端业务结果；`EnsureSuccess()` 在业务失败时抛出 `GatewayRequestException`。网络断开、超时、取消分别通过异常反馈。`RequestAsync<T>` 可传入 `timeout` 和 `cancellationToken`。

## 重连与登录状态

`AutoReconnect` 默认关闭。需要持续重连时，显式打开并提供恢复回调：

```csharp
var credentials = new LoginMessage { Username = "mono", Password = "123456" };
var client = new NetClient(new NetClientOptions
{
    Endpoint = "wss://game.example.com:443/ws",
    AutoReconnect = true,
    SessionRecoveryHandler = async (gateway, cancellationToken) =>
    {
        var result = await gateway.RequestAsync<LoginMessage>(1, 1, credentials,
            cancellationToken: cancellationToken);
        result.EnsureSuccess();
    }
});
await client.ConnectAsync();
// 首次登录由业务执行；恢复回调只在成功连接之后的重连中执行。
(await client.RequestAsync<LoginMessage>(1, 1, credentials)).EnsureSuccess();
```

重连经过 `Reconnecting → Recovering → Connected`，恢复回调内可调用客户端请求接口。回调应遵守传入的取消令牌。服务端 attachment 保留在网关/业务调用链内，当前 wire v1 没有传输 attachment 字段，客户端无需也不能回传它。

服务端踢人通过关闭连接实现，wire v1 不包含踢人原因。业务需要显示原因时，应先推送业务通知，再踢人；收到通知后可调用 `DisconnectAsync()` 停止自动重连，避免反复顶号。UDP 无连接关闭通知，依靠心跳超时或 PONG 中会话被替换来识别失效；UDP 不提供可靠性、顺序、去重和自动重发保障。

失败的业务请求不会自动重放。交易、发奖等写操作的重试和幂等键应由业务协议定义。`ConnectAsync` 的取消令牌只取消本次等待；`DisconnectAsync` 停止整个连接及重连循环。`Dispose` 立即终止网络活动，要求等待后台循环完全退出时先调用 `DisconnectAsync`。

## 载荷和线程约定

| 服务端载荷 | 客户端类型 |
| --- | --- |
| `@ProtobufClass` POJO | 字段号一致的 `[ProtoContract]` 类或生成的 `IMessage` 类 |
| 普通 Action 的 `String` 返回值 | `string`，按 UTF-8 解码 |
| `broadcast` / `PlayerBroadcaster` 的字符串 | `Google.Protobuf.WellKnownTypes.StringValue` |
| 普通 Action 的 int/long/bool/double/float | 对应 C# 类型，按 Java `PrimitiveValue` 解码 |
| 广播的基础类型 | 对应 Google `Int32Value` / `Int64Value` / `BoolValue` / `DoubleValue` / `FloatValue` |
| `byte[]` 或自定义二进制 | `byte[]` |

同一 `cmd/act` 应约定固定载荷类型。仓库演示接口 `1/5` 故意将字符串和 `LoginMessage` 都广播到 `200001`，此例需订阅原始包，由业务区分；客户端无法从没有类型标识的字节中推断消息类型。需要自定义序列化时，实现 `IPayloadCodec` 并配置 `PayloadCodec`。

未指定 `CallbackContext` 时，事件可能来自网络线程或调用线程；指定后只有通知回调被投递，请求 Task 的执行不依赖主线程轮询。回调中避免 `.Wait()` / `.Result`，并在游戏主线程创建客户端时捕获当前同步上下文。此模块面向 Mono/常规 .NET；Unity WebGL 不具备这里使用的套接字 API，IL2CPP/AOT 的 protobuf-net 类型保留和预生成需要由宿主项目配置。

默认限制与当前 gateway 对齐：总帧 1 MiB（含 TCP 长度前缀）、payload 512 KiB、单字符串 8 KiB。UDP 单包另限 65507 字节，实际网络中建议显著减小。`HeartbeatInterval = TimeSpan.Zero` 关闭周期心跳，但仍执行首次会话握手。所有长度在分配和解码前校验。

## Java 端到端验证

需要 PowerShell 7、Java 25、Maven 和 .NET 8+：

```powershell
pwsh -File tests/interop/run-interop.ps1
```

脚本构建所需 Java 模块，使用独立临时端口启动真实 `RegistryServer`、`LoginAction` 和 Netty gateway，分别验证 TCP、UDP、WebSocket 的登录、鉴权、attachment 连续性、推送、广播、顶号及注销，最后关闭本次启动的进程。已构建 Java 模块时可加 `-SkipJavaBuild`。日志位于忽略提交的 `artifacts/`。TLS 的不受信任证书拒绝行为由 C# 套接字测试覆盖；有效生产证书的 `tcps/wss` 连接仍需在部署环境验证。
