# kynet-codegen

扫描服务端 `@ActionController` / `@ActionMethod`，自动生成 **proto 文件** 与 **各语言客户端快捷类**（消息编解码 + action 代理），消灭手写消息编解码和字段号对照。

注解只有 `value` 一种写法（缺省直接编译失败）：

```java
@ActionController(2)              // value 即网关路由的 cmd
public final class SignAction {
    @ActionMethod(1)              // value 即网关路由的 act
    public SignResultMessage sign(SignTokenMessage token) { ... }
}
```

## 架构

```
服务端编译产物(classpath)
   │  ① 采集器（ClassGraph 式反射扫描，initialize=false 不触发静态块）
   ▼
actions.json（语言无关 IR 契约：actions + messages + enums + warnings）
   │  ② 渲染器 --lang proto | csharp | typescript | gdscript
   ▼
kynet-actions.proto + 消息类 + XxxProxy / XxxAction
```

规则复刻 jprotobuf 2.4.23 实测行为：

- `@Protobuf(order = N)` 直接作为字段号；`order` 缺省时按 **max(显式 order)+1** 起依声明顺序递增，全缺省从 1 开始（已用编码字节实证）。
- `@Protobuf(fieldType)` 缺省（`DEFAULT`）按 Java 类型推断（String→STRING、int→INT32、枚举→ENUM、`@ProtobufClass`→OBJECT 等）。

## 用法

### Java 项目内调用

在服务端项目中引入 `kynet-codegen` 后，先编译 Action 类，再运行一个普通的 Java 入口。默认从运行时 classpath 扫描可见的 Action，生成 C# 到调用入口所属项目的 `generated/kynet` 目录：

```java
import cn.itsub.kynet.codegen.CodeGenHelper;

public class GenerateClient {
    public static void main(String[] args) throws Exception {
        CodeGenHelper.generate();
    }
}
```

可选配置按需覆盖默认值；相对输出路径以项目根目录为基准：

```java
import cn.itsub.kynet.codegen.ClientTarget;
import cn.itsub.kynet.codegen.CodeGenHelper;
import java.nio.file.Path;

public class GenerateClient {
    public static void main(String[] args) throws Exception {
        CodeGenHelper.configure()
            .target(ClientTarget.CSHARP)
            .outputDirectory(Path.of("../kynet-client-mono/src/Kynet.Client/Generated"))
            .includePackages("cn.itsub.game.login.action")
            .csharpNamespace("Game.Client.Generated")
            .generate();
    }
}
```

`targets(ClientTarget.CSHARP, ClientTarget.TYPESCRIPT)` 可一次生成多个目标；还支持 `GDSCRIPT` 和 `PROTO`。默认扫描运行时可见的全部 Action，无需设置包名或 classpath；特殊布局可用 `projectRoot(...)`、`classpathEntries(...)` 覆盖自动发现。其他可选项对应 CLI 的 `protoPackage(...)`、`tsImportPrefix(...)`、`gdAddonPath(...)` 和 `gdGeneratedPath(...)`。`scan()` 返回 IR，`scanTo(...)` 保存 actions.json，`fromIr(...)` 从已有 IR 生成。Helper 返回输出文件列表，错误以异常报告，不退出 JVM。

### 命令行

```bash
mvn -pl kynet-codegen package -DskipTests   # 产出可执行 fat jar

# 一步到位（扫描 + 生成）
java -jar kynet-codegen/target/kynet-codegen-2.0.3.jar all \
  --classpath examples/example-login-service/target/classes \
  --lang proto --lang csharp --lang typescript --lang gdscript \
  --out <输出目录>

# 分两步（IR 可入库 review）
java -jar kynet-codegen-2.0.3.jar scan --classpath <目录或jar>... [--include <包前缀>]... --out actions.json
java -jar kynet-codegen-2.0.3.jar gen  --ir actions.json --lang csharp --out <目录> \
  [--namespace Kynet.Client.Generated] [--proto-package kynet.gen] \
  [--ts-import-prefix ..] [--gd-addon-path res://addons/kynet] [--gd-generated-path res://addons/kynet/generated]
```

## 生成物

| 语言 | 文件 | 说明 |
|---|---|---|
| proto | `kynet-actions.proto` | 字段号与 order 一致；非标准载荷（裸字符串 / PrimitiveValue / List 信封）用具名消息 + wire 注释如实标注 |
| C# | `KynetMessages.g.cs`、`<Xxx>Proxy.g.cs`、`KynetGeneratedRuntime.g.cs` | protobuf-net `[ProtoContract]` POCO + 静态代理（`Xxx` 回调 / `XxxAsync` await 双风格）；`KynetGeneratedRuntime.DefaultClient` 设默认 `NetClient`，也可逐调用传入 |
| TS | `kynet-messages.ts`、`<xxx-action>.ts` | protobufjs minimal 风格编解码 + `TypedResponse<T>`（`data` / `ensureSuccess()` / `log()`）；业务方法使用 camelCase，返回 Promise，支持 `await` / `.then()` |
| GDScript | `kynet_messages.gd`、`<xxx_action>.gd` | 复用 `addons/kynet/protobuf.gd` 原语，消息用 Dictionary（键 = Java 字段名）；业务方法使用 snake_case，仅生成 `xxx` await 入口 |

例如服务端 `SignAction.sign(token)` 对应：

```csharp
var response = await SignActionProxy.SignAsync(token);
SignActionProxy.Sign(token, response => response.Log());
```

```typescript
const response = await SignAction.sign(client, token);
SignAction.sign(client, token).then(response => response.log()).catch(console.error);
```

```gdscript
var response = await SignAction.sign(client, token)
```

上述命名替代旧的 `OfXxx` / `OfAwaitXxx`、`ofXxx` / `ofAwaitXxx`、`of_xxx` / `of_await_xxx`，重新生成后调用方需同步迁移。服务端业务方法名及 cmd/act 路由不变。

生成代码为强类型：服务端改参数/返回类型并重新生成后，客户端调用点会在**编译期**报错；`GetValue<T>()`（C# 别名）与 `log()` 兼容 ioGame 式调用习惯。

## 契约与限制（采集期直接报错或警告）

支持：`@ProtobufClass` 消息（含嵌套/repeated/枚举字段）、`String`（裸 UTF-8）、`byte[]`（裸字节）、`int/long/boolean/double/float`（PrimitiveValue 信封）、**List 返回值**（`ListValue` 信封，服务端自动包装）。

- **List 只支持作为返回值**，不支持作为参数（服务端无法解码泛型参数），采集期报错。
- `ActionResult` 返回值：线上载荷是 `getData()` 的运行时值，静态推断不出类型 → 按无数据响应生成并输出**警告**；建议改用直接返回值（如 `examples/.../SignAction`）。
- 枚举 / List 不能直接作 action 参数或返回值，需用消息包装（枚举可作消息字段）。
- 暂不支持：Map、Date、BigDecimal、BigInteger、FieldType 的 SINT/FIXED/UINT 族、消息继承。采集期明确报错，不生成错误代码。
- cmd/act 冲突、字段号冲突、消息/枚举/方法 simple name 冲突均在采集期校验。

## 端到端验证

`clients/kynet-client-mono/tests/interop/CodegenInteropServer.java`（单 JVM 起 registry/gateway/LoginAction/SignAction）+ `CodegenSmoke`（C#）与 `clients/kynet-client-typescript/tests/interop/verify-codegen.mjs`（TS）用生成代理对真实服务器完成消息 / PrimitiveValue / ListValue 三种载荷往返断言。

GDScript 使用 `clients/kynet-client-gdscript/tests/interop/verify_codegen.gd` 对同一服务端验证 await 调用的消息 / PrimitiveValue / ListValue 返回。命令行运行前先执行 `godot --headless --editor --path clients/kynet-client-gdscript --import --quit` 注册生成类，再执行 `godot --headless --path clients/kynet-client-gdscript --script res://tests/interop/verify_codegen.gd -- tcp://127.0.0.1:<网关端口>`。

> 提示：改动框架注解等被 shade 进 fat jar 的类后，重新打包请用 `clean package`——增量 shade 会残留旧类条目导致运行时加载到旧版本。
