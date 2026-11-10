# example-rpc-action-call

演示通过 `ServiceNode.invoke(...)` 调用 action 的完整链路，包含三种场景：

- 成功调用（`act=1`）
- 业务异常（`act=2`）
- 超时返回（`act=3`）

## 启动顺序

1. 启动 broker
2. 启动 provider
3. 启动 caller

## 运行命令（在 `kynet` 根目录）

```bash
mvn -pl kynet-registry -am compile exec:java -Dexec.mainClass="cn.itsub.kynet.registry.RegistryMain"
```

```bash
mvn -pl kynet-examples/example-rpc-action-call -am compile exec:java -Dexec.mainClass="cn.itsub.game.rpc.demo.RpcActionProviderApp"
```

```bash
mvn -pl kynet-examples/example-rpc-action-call -am compile exec:java -Dexec.mainClass="cn.itsub.game.rpc.demo.RpcActionCallerApp"
```

## 预期输出（caller）

- `[success-case] status=SUCCESS, detail=echo:hello`
- `[error-case] code=ACTION_EXECUTION_ERROR, message=...业务校验失败...`
- `[timeout-case] code=TIMEOUT, message=RPC timeout after 200 ms`

## action 说明

- `cmd=10, act=1`：返回 `RpcDemoResponse`
- `cmd=10, act=2`：抛出业务异常，用于演示错误路径
- `cmd=10, act=3`：sleep 800ms，用 200ms 超时触发 `TIMEOUT`
- `cmd=101, act=1`：最简单的转发压测 action，接收固定请求并返回 `RpcDemoResponse`

## broker 转发压测

1. 启动 broker
2. 启动 `ForwardEchoProviderApp`
3. 启动 `BrokerForwardBurstCallerApp`

```bash
mvn -pl kynet-examples/example-rpc-action-call -am compile exec:java -Dexec.mainClass="cn.itsub.game.rpc.demo.app_forward_burst.ForwardEchoProviderApp"
```

```bash
mvn -pl kynet-examples/example-rpc-action-call -am compile exec:java -Dexec.mainClass="cn.itsub.game.rpc.demo.app_forward_burst.BrokerForwardBurstCallerApp"
```

默认行为：

- 10 个线程
- 持续压测 5 秒
- 固定请求内容：`forward-benchmark`
- 输出每个线程的 `qps/success/failed` 和总 `qps`

## invokeOneWay 案例

1. 启动 broker
2. 启动 `RpcOneWayReceiverApp`
3. 启动 `RpcOneWaySenderApp`

```bash
mvn -pl kynet-examples/example-rpc-action-call -am compile exec:java -Dexec.mainClass="cn.itsub.game.rpc.demo.RpcOneWayReceiverApp"
```

```bash
mvn -pl kynet-examples/example-rpc-action-call -am compile exec:java -Dexec.mainClass="cn.itsub.game.rpc.demo.RpcOneWaySenderApp"
```

默认行为：

- 发送端 4 个线程
- 持续发送 5 秒
- 固定请求内容：`invoke-one-way`
- 发送端输出每个线程的 `qps/sent/failed` 和总 `qps`
- 接收端从收到第一条请求开始计时，统计 3 秒窗口内的接收 `qps`

## 极限 QPS 压测（连接已启动的 broker）

`BrokerActionMaxQpsTest` 不内嵌 broker，直接连接正在运行的 broker（默认 `127.0.0.1:8888`）：
服务 A 注册零业务逻辑的 INLINE Action，服务 B 用 prepared sender 极限调用。包含两个用例：

- `measuresMaxActionQpsAgainstRunningBroker`：RPC 往返 QPS
- `measuresMaxOneWayQpsAgainstRunningBroker`：one-way 单向 QPS（`prepareFastOneWaySender` + `FastOneWayHandler`）

```bash
mvn -pl kynet-examples/example-rpc-action-call -am test -Dtest=BrokerActionMaxQpsTest
```

连不上 broker 时测试自动 skip。可调参数：

- `kynet.broker.external.host` / `kynet.broker.external.port`
- `kynet.broker.maxQps.senderThreads`（默认 min(CPU 核数, 8)）
- `kynet.broker.maxQps.maxInFlight`（RPC 在途窗口，默认 8192）
- `kynet.broker.maxQps.warmupSeconds` / `durationSeconds`

本机实测（8 线程、10 秒窗口、IPC）：RPC 往返约 `2.7M QPS`，one-way 约 `8.2M QPS`，
零失败、零丢弃。裸 Aeron IPC 同机约 `14M QPS`，其余开销主要来自单收包线程的帧解析与事务分发；
继续冲高需要批量帧或多收包线程。
