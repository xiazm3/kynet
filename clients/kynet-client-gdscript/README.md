# Kynet Godot GDScript 客户端

可移植的网络组件位于 [`addons/kynet/`](addons/kynet/README.md)。复制该目录即可接入其他 Godot 4 工程，无需 C#、.NET 或第三方插件。API、载荷格式及重连配置见组件文档。

## 示例

先启动本仓库的注册中心、登录服务和网关，在编辑器中运行 `examples/login.tscn`，或执行：

```powershell
godot --path . res://examples/login.tscn
```

示例使用 `tcp://127.0.0.1:30001` 和演示账号 `godot` / `123456`，执行登录、接收欢迎推送、hello、注销。修改 `examples/login.gd` 可切换服务器地址。网络组件不依赖示例场景。

## 测试

协议与本地网络测试不需要启动 Java 服务：

```powershell
godot --headless --path . --script tests/run_tests.gd
```

覆盖 Java/C# 协议字节向量、64 位 ID、长度和 UTF-8 校验、Protobuf、TCP 拆包/粘包、超时取消、订阅、会话替换、断线清理、节点释放、队列上限、重连恢复、恢复超时及心跳检测。TCPS/WSS 使用本地自签名证书验证不受信任证书拒绝和显式 CA 信任后的加密请求；测试中的证书拒绝会产生预期的 Godot TLS 错误日志，以最终 `TESTS: ... 0 failures` 和退出码 0 为准。

真实 Java 网关联调需要 PowerShell 7、Java 25、Maven、Godot 4：

```powershell
./tests/interop/run-interop.ps1 -GodotPath 'C:/path/to/godot_console.exe'
# Java 模块已构建时：
./tests/interop/run-interop.ps1 -GodotPath 'C:/path/to/godot_console.exe' -SkipJavaBuild
```

脚本复用 C# 客户端的 Java 联调服务，启动真实 RegistryServer、LoginAction 和 Netty 网关，在临时本机端口分别验证 TCP、UDP、WebSocket 的握手、鉴权错误、登录、推送、attachment 连续性、广播、顶号和注销，最后关闭本次启动的进程。日志写入忽略提交的 `artifacts/`。可通过 `-ClasspathFile` 指定已有 Java classpath 文件。

开发验证使用 Godot 4.7。生产 TCPS/WSS 的证书链和 HTML5 导出还应在目标部署环境验证。
