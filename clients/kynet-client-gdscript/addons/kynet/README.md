# Kynet GDScript 网络组件

纯 GDScript 的 Godot 4 网络组件，连接本仓库 `GatewayServer`，兼容现有 C# 客户端使用的 gateway wire v1。运行时不依赖 C#、.NET、第三方插件或编辑器插件。

## 移植

将整个 `addons/kynet/` 目录复制到目标 Godot 工程。目录内部使用相对 `preload`，可以整体改名或移动；业务代码只需要调整入口脚本路径。无需在编辑器中启用插件。

```gdscript
const KynetClient = preload("res://addons/kynet/client.gd")
const Protobuf = preload("res://addons/kynet/protobuf.gd")

var network = KynetClient.new()

func _ready() -> void:
    add_child(network)
    network.subscribe(5, 6, _on_welcome)
    var connected: Dictionary = await network.connect_async("tcp://127.0.0.1:30001")
    if not connected.ok:
        push_error(connected.error_message)
        return

    var login: Dictionary = await network.request_async(
        1, 1, Protobuf.login_message("godot", "123456"))
    if not login.ok:
        push_error(str(login.error_code, ": ", login.error_message))
        return
    print("user_id=", network.user_id)
    print("token=", Protobuf.string_value(login.payload, 3).value)

    var hello: Dictionary = await network.request_async(1, 2)
    if hello.ok:
        print(hello.payload.get_string_from_utf8())

func _on_welcome(packet: Dictionary) -> void:
    var text := Protobuf.string_value(packet.payload)
    if text.ok:
        print(text.value)
```

也可将 `client.gd` 配置为 Autoload。所有接口、信号、订阅回调都在 Godot 主线程使用；节点必须先进入场景树。组件使用非阻塞轮询，场景树暂停时仍处理网络，节点退出场景树时自动关闭连接并完成未结束的请求。需要跨场景保持连接时使用 Autoload。

## 接口

| 接口 | 返回与用途 |
| --- | --- |
| `connect_to_gateway(endpoint = "")` | 返回连接 Operation；省略 endpoint 使用 options.endpoint |
| `connect_async(endpoint = "")` | 等待首次 PING/PONG 完成，返回结果 Dictionary |
| `disconnect_from_gateway()` | 立即断开，清空会话及未完成请求，停止自动重连 |
| `request(cmd, act, payload = null, timeout_ms = -1)` | 返回 Operation，可保存后取消 |
| `request_async(cmd, act, payload = null, timeout_ms = -1)` | 等待响应，返回结果 Dictionary |
| `request_message(message_id, payload = null, timeout_ms = -1)` | 按 messageId 请求，返回 Operation |
| `send_one_way(cmd, act, payload = null)` | 入队成功返回 `OK`，无响应等待；发送失败通过连接错误通知 |
| `send_one_way_message(message_id, payload = null)` | 按 messageId 单向发送 |
| `ping()` | 返回 PING/PONG Operation，可通过 `await network.ping().wait()` 等待 |
| `subscribe(cmd, act, handler)` / `unsubscribe(cmd, act, handler)` | 订阅/取消对应推送，handler 接收完整包 Dictionary |
| `subscribe_message(message_id, handler)` / `unsubscribe_message(message_id, handler)` | 按 messageId 订阅/取消推送 |

`messageId = (cmd << 16) | act`。支持 Java/C# 有符号 32 位 messageId，也支持对应的非负 32 位数；请求的 cmd、act 范围为 1..65535。重复订阅同一个 Callable 不会重复调用。取消订阅时使用原 Callable。

Operation 的 `wait()` 支持在操作已经完成之后调用，不会漏掉信号；`completed(result)` 发出一次，`is_completed` 和 `result` 可直接检查。

```gdscript
var pending = network.request(1, 2, null, 1500)
# 在取消按钮等回调中：pending.cancel()
var response: Dictionary = await pending.wait()
```

调用连接 Operation 的 `cancel()` 会断开共享连接并停止重连，连接结果为 `DISCONNECTED`。请求 Operation 的 `cancel()` 返回 `CANCELLED`。取消/超时仅结束本地等待，已经发往服务器的业务仍可能执行；业务重试需自行设计幂等键。已部分写出的 TCP/TLS 帧被取消时会关闭连接，避免破坏后续帧。

## 结果和信号

连接与请求结果都有 `ok`、`error_code`、`error_message`、`transport_error`、`payload`、`session_id`、`user_id`。请求成功时 payload 为原始 `PackedByteArray` 或 null；收到业务错误时 `transport_error = false`，保留服务端原始错误码。组件本地错误的 `transport_error = true`，例如 `NOT_CONNECTED`、`TIMEOUT`、`CANCELLED`、`QUEUE_FULL`、`SEND_ERROR`、`DISCONNECTED`、`PROTOCOL_ERROR`。`ok` 根据 error_code 是否为 null 判断，空字符串错误码仍是业务失败。

完整包还包括 `type`、`one_way`、`request_id`、`cmd`、`act`、`timeout_ms`。会话 ID 和用户 ID 全程使用 64 位整数，避免通过 JSON 浮点数丢失精度。

信号：`state_changed(state)`、`session_changed(session_id, user_id)`、`push_received(packet)`、`connection_error(code, message)`。业务请求错误通过 Operation 返回；连接错误额外发送 `connection_error`。状态枚举为 `DISCONNECTED`、`CONNECTING`、`RECOVERING`、`CONNECTED`、`RECONNECTING`。

## 配置与重连

连接前修改 `network.options`；每次显式连接时复制配置，运行中修改 options 不影响当前连接及其重连。首次连接失败会直接结束连接操作；首次连接成功后，`auto_reconnect` 才生效。订阅跨重连保留，未完成请求在断线时失败，不会自动重放。

```gdscript
network.options.endpoint = "wss://game.example.com:443/ws"
network.options.auto_reconnect = true
network.session_recovery_handler = func(gateway):
    var login: Dictionary = await gateway.request_async(
        1, 1, Protobuf.login_message("godot", "123456"))
    return login.ok
```

恢复回调只在重连时调用，可以 await 请求，必须返回 bool。恢复完成前处于 `RECOVERING`，此时允许业务请求；返回 true 后进入 `CONNECTED`，失败或超时则继续退避重连。首次登录由业务执行。收到顶号业务通知后可手动断开，避免自动重连反复顶号。wire v1 只有连接关闭，没有踢人原因字段。

| 配置 | 默认值 |
| --- | --- |
| `endpoint` | `tcp://127.0.0.1:30001` |
| `connect_timeout_ms` | 5000，分别限制传输连接和首次 PING/PONG |
| `recovery_timeout_ms` | 10000，限制恢复回调 |
| `request_timeout_ms` | 3000 |
| `heartbeat_interval_ms` / `heartbeat_timeout_ms` | 5000 / 3000；interval 为 0 仅禁用周期心跳 |
| `auto_reconnect` | false |
| `reconnect_initial_delay_ms` / `reconnect_max_delay_ms` | 1000 / 15000，指数退避 |
| `max_pending_requests` / `max_pending_writes` | 1024 / 256 |
| `max_frame_length` / `max_payload_length` / `max_string_length` | 1 MiB / 512 KiB / 8 KiB |
| `max_packets_per_poll` | 128；每帧有界收发，TCP 发送另有字节预算 |
| `tls_options` | null，使用 Godot 默认 CA 和主机名校验；私有 CA 可设为 `TLSOptions.client(certificate)` |

支持 `tcp://`、`tcps://`、`udp://`、`ws://`、`wss://`，均需显式端口。TCP/UDP 不接受路径或 query；IPv6 地址加方括号。TCP/TLS 使用 4 字节大端长度前缀；UDP/WS 每包对应一个 wire 数据包。帧上限包含 TCP 长度前缀，与网关一致；长度在读取包体前检查，null 和空字节数组保持区别。

UDP 单包最多 65507 字节，且不保证可靠性、顺序、去重或重发；建议进一步减小业务包。UDP 顶号/失联通过心跳超时或 PONG 中的会话替换识别。HTML5 导出使用 ws/wss，原生 TCP/UDP 不可用；实际部署需配置浏览器信任的证书和 HTTPS 对应的 wss。

## 业务载荷

网络组件仅接收 `PackedByteArray` 或 null。`protobuf.gd` 提供轻量字段工具，可以用其他 GDScript Protobuf 库生成的字节替代，不耦合具体生成器。

| 服务端返回类型 | 解码方式 |
| --- | --- |
| 普通 Action 的 String | `response.payload.get_string_from_utf8()` |
| 广播/PlayerBroadcaster 的字符串 | Google StringValue，`Protobuf.string_value(payload)` |
| 示例 LoginMessage | 字段 1=username、2=password、3=token；`login_message()` 编码，`string_value(payload, field)` 读取 |
| 普通 Action 的 int/long/bool/double/float | Java PrimitiveValue，`encode_primitive(value, kind)` / `decode_primitive(payload, kind)`，kind 依次为 0..4 |
| 其他 POJO 或 Protobuf 消息 | 按服务端字段号编码，或接入生成器；原始二进制直接使用 |

`decode_fields()` 返回 `{ok, fields}`，fields 按字段号保存数组，每项为 `{wire, value}`。支持 wire 0 的 varint、wire 1 的 double、wire 2 的 bytes、wire 5 的 float；嵌套消息可对 bytes 再次解码。它不提供完整 schema、map、oneof、packed repeated、fixed 整数或 proto2 group 语义。复杂业务协议建议使用生成器。Google 数值 wrapper 和 Java PrimitiveValue 格式不同，不能混用。

同一消息号需要约定固定载荷类型。示例 `1/5` 将 StringValue 和 LoginMessage 混合广播到 `200001`，必须由业务区分，网络层无法推断字节的具体消息类型。服务端 attachment 只在网关/服务内部流转，wire v1 不包含此字段。
