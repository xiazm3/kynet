package cn.itsub.kynet.codegen.render;

import cn.itsub.kynet.codegen.ir.ActionMeta;
import cn.itsub.kynet.codegen.ir.EnumMeta;
import cn.itsub.kynet.codegen.ir.FieldMeta;
import cn.itsub.kynet.codegen.ir.IrModel;
import cn.itsub.kynet.codegen.ir.MessageMeta;
import cn.itsub.kynet.codegen.ir.TypeKind;
import cn.itsub.kynet.codegen.ir.TypeRef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 生成 protobuf-net POCO 消息类 + 每个控制器一个静态代理类（回调/await 双风格）。
 * 目标 netstandard2.0 / C# 8 / Nullable enable / TreatWarningsAsErrors，生成代码必须零警告。
 */
public final class CSharpRenderer implements Renderer {
    private final List<Path> written = new ArrayList<>();
    private Map<String, String> messageNames = new LinkedHashMap<>();
    private Map<String, EnumMeta> enumByClass = new LinkedHashMap<>();

    @Override
    public List<Path> render(IrModel model, RenderOptions options, Path outDir) throws IOException {
        written.clear();
        messageNames = new LinkedHashMap<>();
        enumByClass = new LinkedHashMap<>();
        for (MessageMeta message : model.messages()) {
            messageNames.put(message.javaClass(), message.name());
        }
        for (EnumMeta enumMeta : model.enums()) {
            enumByClass.put(enumMeta.javaClass(), enumMeta);
        }

        Files.createDirectories(outDir);
        String ns = options.csharpNamespace();
        Path runtime = outDir.resolve("KynetGeneratedRuntime.g.cs");
        Files.writeString(runtime, runtimeFile(ns, options));
        written.add(runtime);

        Path messagesFile = outDir.resolve("KynetMessages.g.cs");
        Files.writeString(messagesFile, messagesFile(model, ns, options));
        written.add(messagesFile);

        Map<String, List<ActionMeta>> byController = groupByController(model.actions());
        for (Map.Entry<String, List<ActionMeta>> entry : byController.entrySet()) {
            String controller = entry.getKey();
            Path proxy = outDir.resolve(controller + "Proxy.g.cs");
            Files.writeString(proxy, proxyFile(ns, controller, entry.getValue(), options));
            written.add(proxy);
        }
        return List.copyOf(written);
    }

    // ---------------- runtime ----------------

    private String runtimeFile(String ns, RenderOptions options) {
        return header(options)
                + "using System;\nusing Kynet.Client;\n\nnamespace " + ns + "\n{\n"
                + "    /// <summary>生成代理使用的默认 NetClient；未配置时调用将抛出异常。</summary>\n"
                + "    public static class KynetGeneratedRuntime\n    {\n"
                + "        public static NetClient? DefaultClient { get; set; }\n\n"
                + "        public static NetClient Resolve(NetClient? client) =>\n"
                + "            client ?? DefaultClient ?? throw new InvalidOperationException(\n"
                + "                \"未配置 NetClient：请在调用时传入，或设置 KynetGeneratedRuntime.DefaultClient。\");\n"
                + "    }\n}\n";
    }

    // ---------------- messages ----------------

    private String messagesFile(IrModel model, String ns, RenderOptions options) {
        StringBuilder out = new StringBuilder(header(options))
                .append("using System;\nusing System.Collections.Generic;\nusing System.IO;\nusing ProtoBuf;\n\n")
                .append("namespace ").append(ns).append("\n{\n");

        out.append(listValueContract());
        out.append(listValueDecoder());

        for (EnumMeta enumMeta : model.enums()) {
            String owner = enumOwnerClass(enumMeta.javaClass());
            if (owner != null && messageNames.containsKey(owner)) {
                continue; // 挂在其所属消息类内部输出
            }
            out.append(enumBlock(enumMeta, "    "));
        }

        for (MessageMeta message : model.messages()) {
            out.append("    /// <summary>对应 Java ").append(message.javaClass()).append("</summary>\n")
                    .append("    [ProtoContract]\n")
                    .append("    public sealed class ").append(message.name()).append("\n    {\n");
            for (EnumMeta enumMeta : model.enums()) {
                if (message.javaClass().equals(enumOwnerClass(enumMeta.javaClass()))) {
                    out.append(enumBlock(enumMeta, "        "));
                }
            }
            for (FieldMeta field : message.fields()) {
                out.append("        ").append(csharpFieldType(field)).append('\n');
            }
            out.append("    }\n");
        }
        out.append("}\n");
        return out.toString();
    }

    private String listValueContract() {
        return "    /// <summary>对应 Java cn.itsub.kynet.common.codec.ListValue（List 载荷信封）。</summary>\n"
                + "    [ProtoContract]\n"
                + "    public sealed class KynetListValue\n    {\n"
                + "        public enum ListValueKind { String = 0, Int = 1, Long = 2, Bool = 3, Double = 4, Float = 5, Message = 6 }\n\n"
                + "        [ProtoMember(1, IsRequired = true)] public int Kind { get; set; }\n"
                + "        [ProtoMember(2)] public List<string>? StringValues { get; set; }\n"
                + "        [ProtoMember(3)] public List<int>? IntValues { get; set; }\n"
                + "        [ProtoMember(4)] public List<long>? LongValues { get; set; }\n"
                + "        [ProtoMember(5)] public List<bool>? BoolValues { get; set; }\n"
                + "        [ProtoMember(6)] public List<double>? DoubleValues { get; set; }\n"
                + "        [ProtoMember(7)] public List<float>? FloatValues { get; set; }\n"
                + "        [ProtoMember(8)] public List<byte[]>? MessageValues { get; set; }\n"
                + "        [ProtoMember(9)] public string? ElementType { get; set; }\n"
                + "    }\n\n";
    }

    private String listValueDecoder() {
        return "    /// <summary>KynetListValue 到客户端 List 的解码辅助。</summary>\n"
                + "    public static class KynetListValueDecoder\n    {\n"
                + "        public static List<string> ToStringList(KynetListValue value) => Kind(value, KynetListValue.ListValueKind.String).StringValues ?? new List<string>();\n"
                + "        public static List<int> ToIntList(KynetListValue value) => Kind(value, KynetListValue.ListValueKind.Int).IntValues ?? new List<int>();\n"
                + "        public static List<long> ToLongList(KynetListValue value) => Kind(value, KynetListValue.ListValueKind.Long).LongValues ?? new List<long>();\n"
                + "        public static List<bool> ToBoolList(KynetListValue value) => Kind(value, KynetListValue.ListValueKind.Bool).BoolValues ?? new List<bool>();\n"
                + "        public static List<double> ToDoubleList(KynetListValue value) => Kind(value, KynetListValue.ListValueKind.Double).DoubleValues ?? new List<double>();\n"
                + "        public static List<float> ToFloatList(KynetListValue value) => Kind(value, KynetListValue.ListValueKind.Float).FloatValues ?? new List<float>();\n\n"
                + "        public static List<T> ToMessageList<T>(KynetListValue value) where T : class\n"
                + "        {\n"
                + "            var envelope = Kind(value, KynetListValue.ListValueKind.Message);\n"
                + "            var result = new List<T>();\n"
                + "            if (envelope.MessageValues == null) return result;\n"
                + "            foreach (var bytes in envelope.MessageValues)\n"
                + "            {\n"
                + "                using (var stream = new MemoryStream(bytes, false))\n"
                + "                {\n"
                + "                    result.Add(Serializer.Deserialize<T>(stream));\n"
                + "                }\n"
                + "            }\n"
                + "            return result;\n"
                + "        }\n\n"
                + "        private static KynetListValue Kind(KynetListValue value, KynetListValue.ListValueKind expected)\n"
                + "        {\n"
                + "            if (value == null) throw new ArgumentNullException(nameof(value));\n"
                + "            if ((KynetListValue.ListValueKind)value.Kind != expected)\n"
                + "                throw new InvalidDataException(\"ListValue kind=\" + value.Kind + \" does not match \" + expected);\n"
                + "            return value;\n"
                + "        }\n"
                + "    }\n\n";
    }

    private String enumBlock(EnumMeta enumMeta, String indent) {
        StringBuilder out = new StringBuilder(indent)
                .append("public enum ").append(enumMeta.name()).append("\n").append(indent).append("{\n");
        List<EnumMeta.EnumConstant> constants = enumMeta.constants();
        for (int i = 0; i < constants.size(); i++) {
            EnumMeta.EnumConstant constant = constants.get(i);
            out.append(indent).append("    ").append(constant.name()).append(" = ")
                    .append(constant.ordinal()).append(i + 1 < constants.size() ? "," : "").append('\n');
        }
        out.append(indent).append("}\n\n");
        return out.toString();
    }

    /** 完整属性语句（含结尾分号）；初始化器保证 Nullable + TreatWarningsAsErrors 下零警告。 */
    private String csharpFieldType(FieldMeta field) {
        String type = csharpType(field.kind(), field.message(), field.enumName());
        if (field.repeated()) {
            return "[ProtoMember(" + field.order() + ")] public List<" + type + "> " + Names.pascal(field.name())
                    + " { get; set; } = new List<" + type + ">();";
        }
        String name = Names.pascal(field.name());
        String attribute = "[ProtoMember(" + field.order() + ")] public ";
        return switch (field.kind()) {
            case STRING -> attribute + "string " + name + " { get; set; } = \"\";";
            case BYTES -> attribute + "byte[]? " + name + " { get; set; } = Array.Empty<byte>();";
            case MESSAGE -> attribute + type + "? " + name + " { get; set; }";
            case INT32 -> attribute + "int " + name + " { get; set; } = 0;";
            case INT64 -> attribute + "long " + name + " { get; set; } = 0;";
            case BOOL -> attribute + "bool " + name + " { get; set; } = false;";
            case DOUBLE -> attribute + "double " + name + " { get; set; } = 0d;";
            case FLOAT -> attribute + "float " + name + " { get; set; } = 0f;";
            case ENUM -> attribute + type + " " + name + " { get; set; } = (" + type + ")0;";
        };
    }

    private String csharpType(TypeKind kind, String message, String enumName) {
        return switch (kind) {
            case MESSAGE -> messageNames.getOrDefault(message, Names.pascal(simpleName(message)));
            case STRING -> "string";
            case BYTES -> "byte[]";
            case INT32 -> "int";
            case INT64 -> "long";
            case BOOL -> "bool";
            case DOUBLE -> "double";
            case FLOAT -> "float";
            case ENUM -> enumByClass.containsKey(enumName)
                    ? enumByClass.get(enumName).name() : Names.pascal(simpleName(enumName));
        };
    }

    // ---------------- proxies ----------------

    private String proxyFile(String ns, String controller, List<ActionMeta> actions, RenderOptions options) {
        StringBuilder out = new StringBuilder(header(options))
                .append("using System;\nusing System.Collections.Generic;\nusing System.Threading.Tasks;\n")
                .append("using Kynet.Client;\n\nnamespace ").append(ns).append("\n{\n")
                .append("    /// <summary>").append(actions.get(0).controllerClass()).append(" 的客户端代理。</summary>\n")
                .append("    public static class ").append(controller).append("Proxy\n    {\n");
        for (ActionMeta action : actions) {
            out.append(proxyMethods(action));
        }
        out.append("    }\n}\n");
        return out.toString();
    }

    private static final Set<String> CSHARP_KEYWORDS = Set.of(
            "abstract", "as", "base", "bool", "break", "byte", "case", "catch", "char", "checked", "class",
            "const", "continue", "decimal", "default", "delegate", "do", "double", "else", "enum", "event",
            "explicit", "extern", "false", "finally", "fixed", "float", "for", "foreach", "goto", "if",
            "implicit", "in", "int", "interface", "internal", "is", "lock", "long", "namespace", "new",
            "null", "object", "operator", "out", "override", "params", "private", "protected", "public",
            "readonly", "ref", "return", "sbyte", "sealed", "short", "sizeof", "stackalloc", "static",
            "string", "struct", "switch", "this", "throw", "true", "try", "typeof", "uint", "ulong",
            "unchecked", "unsafe", "ushort", "using", "var", "virtual", "void", "volatile", "while");

    /** C# 保留字做参数名时加 @ 前缀（int -> @int）。 */
    private String csharpIdentifier(String name) {
        if (name != null && CSHARP_KEYWORDS.contains(name)) {
            return "@" + name;
        }
        return name;
    }

    private String proxyMethods(ActionMeta action) {
        String rpc = Names.pascal(action.methodName());
        String callbackMethod = rpc;
        String asyncMethod = rpc + "Async";
        String paramName = action.paramName() == null ? "" : csharpIdentifier(action.paramName());
        String csParam = action.paramType() == null ? "" : csharpType(action.paramType().kind(),
                action.paramType().message(), action.paramType().enumName()) + " " + paramName;
        String argName = paramName.isEmpty() ? "" : ", " + paramName;

        // await 版本
        String requestType = requestType(action.returnType());
        String responseType = responseType(action.returnType());
        StringBuilder out = new StringBuilder();
        out.append("        /// <summary>cmd=").append(action.cmd()).append(" act=").append(action.act())
                .append(action.needsContext() ? "（服务端使用 ActionContext）" : "").append("</summary>\n");
        out.append("        public static async Task<").append(responseType).append("> ").append(asyncMethod)
                .append('(').append(csParam).append(csParam.isEmpty() ? "" : ", ")
                .append("NetClient? client = null)\n        {\n")
                .append("            var net = KynetGeneratedRuntime.Resolve(client);\n");
        if (action.returnType() != null && action.returnType().repeated()) {
            out.append("            var envelope = await net.RequestAsync<").append(requestType).append(">(")
                    .append(action.cmd()).append(", ").append(action.act()).append(argName)
                    .append(").ConfigureAwait(false);\n")
                    .append("            var data = ").append(decodeList(action.returnType()))
                    .append(";\n")
                    .append("            return new ActionResponse<")
                    .append("List<").append(csharpType(action.returnType().kind(),
                            action.returnType().message(), action.returnType().enumName()))
                    .append(">>(data, envelope.ErrorCode, envelope.ErrorMessage, envelope.SessionId, envelope.UserId);\n");
        } else {
            out.append("            return await net.RequestAsync<").append(requestType).append(">(")
                    .append(action.cmd()).append(", ").append(action.act()).append(argName)
                    .append(").ConfigureAwait(false);\n");
        }
        out.append("        }\n\n");

        // 回调版本
        out.append("        /// <summary>回调风格：").append(callbackMethod).append("(参数, result => ...)。</summary>\n")
                .append("        public static async void ").append(callbackMethod).append('(')
                .append(csParam).append(csParam.isEmpty() ? "" : ", ")
                .append("Action<").append(responseType).append("> callback, NetClient? client = null)\n")
                .append("        {\n")
                .append("            try\n")
                .append("            {\n")
                .append("                callback(await ").append(asyncMethod).append('(')
                .append(paramName.isEmpty() ? "client" : paramName + ", client")
                .append("));\n")
                .append("            }\n")
                .append("            catch (Exception exception)\n")
                .append("            {\n")
                .append("                Console.WriteLine(\"[Kynet] \" + exception.Message);\n")
                .append("            }\n")
                .append("        }\n\n");
        return out.toString();
    }

    /** RequestAsync<T> 的 T：List 走信封，MESSAGE/标量走原类型，void 用 object?（泛型不变性需与响应签名一致）。 */
    private String requestType(TypeRef ref) {
        if (ref == null) {
            return "object?";
        }
        if (ref.repeated()) {
            return "KynetListValue";
        }
        return csharpType(ref.kind(), ref.message(), ref.enumName());
    }

    /** 对外响应类型：List 解包成 List<元素>，void 用 object。 */
    private String responseType(TypeRef ref) {
        if (ref == null) {
            return "ActionResponse<object?>";
        }
        if (ref.repeated()) {
            return "ActionResponse<List<" + csharpType(ref.kind(), ref.message(), ref.enumName()) + ">>";
        }
        return "ActionResponse<" + csharpType(ref.kind(), ref.message(), ref.enumName()) + ">";
    }

    private String decodeList(TypeRef ref) {
        return switch (ref.kind()) {
            case STRING -> "KynetListValueDecoder.ToStringList(envelope.EnsureSuccess())";
            case INT32 -> "KynetListValueDecoder.ToIntList(envelope.EnsureSuccess())";
            case INT64 -> "KynetListValueDecoder.ToLongList(envelope.EnsureSuccess())";
            case BOOL -> "KynetListValueDecoder.ToBoolList(envelope.EnsureSuccess())";
            case DOUBLE -> "KynetListValueDecoder.ToDoubleList(envelope.EnsureSuccess())";
            case FLOAT -> "KynetListValueDecoder.ToFloatList(envelope.EnsureSuccess())";
            case MESSAGE -> "KynetListValueDecoder.ToMessageList<"
                    + csharpType(ref.kind(), ref.message(), ref.enumName())
                    + ">(envelope.EnsureSuccess())";
            case ENUM -> throw new IllegalStateException("ENUM 列表未支持：" + ref.enumName());
            case BYTES -> throw new IllegalStateException("List<byte[]> 未支持（ListValue 信封无 bytes 槽位）");
        };
    }

    private Map<String, List<ActionMeta>> groupByController(List<ActionMeta> actions) {
        Map<String, List<ActionMeta>> grouped = new LinkedHashMap<>();
        for (ActionMeta action : actions) {
            grouped.computeIfAbsent(action.controllerName(), key -> new ArrayList<>()).add(action);
        }
        return grouped;
    }

    /** 枚举若声明在某消息类内部（Java 嵌套 $），返回其宿主类的全名。 */
    private String enumOwnerClass(String javaClass) {
        int dollar = javaClass.indexOf('$');
        return dollar > 0 ? javaClass.substring(0, dollar) : null;
    }

    private String simpleName(String fqcn) {
        int dot = fqcn.lastIndexOf('.');
        return dot > 0 ? fqcn.substring(dot + 1) : fqcn;
    }

    private String header(RenderOptions options) {
        // 自动生成文件不继承工程 nullable 上下文（CS8669），需要显式指令
        return "// Generated by kynet-codegen at " + options.generatedAt() + ". DO NOT EDIT.\n#nullable enable\n";
    }
}
