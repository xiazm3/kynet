package cn.itsub.kynet.codegen;

import cn.itsub.kynet.codegen.ir.IrModel;
import cn.itsub.kynet.codegen.render.RenderOptions;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * kynet-codegen 命令行：
 * <pre>
 *   scan --classpath <编译产物目录/jar>... [--include <包前缀>]... --out actions.json
 *   gen  --ir actions.json --lang proto|csharp|typescript|gdscript [--lang ...] --out <目录>
 *   all  --classpath ... --lang ... --out ...        （scan+gen 一步完成，不落中间文件）
 * </pre>
 */
public final class CodeGenCli {
    public static void main(String[] args) {
        try {
            run(args);
        } catch (IllegalArgumentException | IllegalStateException e) {
            System.err.println("[kynet-codegen] " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("[kynet-codegen] 意外失败: " + e);
            e.printStackTrace();
            System.exit(2);
        }
    }

    static void run(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            return;
        }
        String command = args[0];
        Map<String, List<String>> options = parseOptions(args);
        switch (command) {
            case "scan" -> scan(options);
            case "gen" -> generate(options);
            case "all" -> all(options);
            default -> usage();
        }
    }

    private static void scan(Map<String, List<String>> options) throws Exception {
        String out = requireOption(options, "--out");
        IrModel model = scanModel(options);
        CodeGenEngine.writeIr(model, Path.of(out));
        System.out.println("[kynet-codegen] 已写出 " + out + "（" + model.actions().size()
                + " 个 action，" + model.messages().size() + " 个消息，" + model.enums().size() + " 个枚举）");
    }

    private static void generate(Map<String, List<String>> options) throws Exception {
        String irPath = requireOption(options, "--ir");
        String out = requireOption(options, "--out");
        IrModel model = CodeGenEngine.readIr(Path.of(irPath));
        renderAll(model, options, Path.of(out));
    }

    private static void all(Map<String, List<String>> options) throws Exception {
        String out = requireOption(options, "--out");
        IrModel model = scanModel(options);
        renderAll(model, options, Path.of(out));
    }

    private static IrModel scanModel(Map<String, List<String>> options) {
        List<String> classpathValues = options.get("--classpath");
        if (classpathValues == null || classpathValues.isEmpty()) {
            throw new IllegalArgumentException("缺少 --classpath");
        }
        List<Path> classpath = classpathValues.stream().map(Path::of).map(Path::toAbsolutePath).toList();
        List<String> includes = options.getOrDefault("--include", List.of());
        return CodeGenEngine.scan(classpath, includes);
    }

    private static void renderAll(IrModel model, Map<String, List<String>> options, Path outDir) throws Exception {
        RenderOptions renderOptions = new RenderOptions(
                single(options, "--namespace", "Kynet.Client.Generated"),
                single(options, "--proto-package", "kynet.gen"),
                single(options, "--ts-import-prefix", ".."),
                single(options, "--gd-addon-path", "res://addons/kynet"),
                single(options, "--gd-generated-path", "res://addons/kynet/generated"),
                Instant.now().toString());
        List<String> langs = options.getOrDefault("--lang", List.of());
        if (langs.isEmpty()) {
            throw new IllegalArgumentException("缺少 --lang（proto/csharp/typescript/gdscript，可重复）");
        }
        for (String lang : langs) {
            CodeGenEngine.RenderResult result = CodeGenEngine.render(model, renderOptions, outDir, List.of(lang)).getFirst();
            System.out.println("[kynet-codegen] " + result.language() + " -> " + outDir + "（"
                    + result.files().size() + " 个文件）");
            for (Path file : result.files()) {
                System.out.println("  " + file.getFileName());
            }
        }
    }

    private static Map<String, List<String>> parseOptions(String[] args) {
        Map<String, List<String>> options = new LinkedHashMap<>();
        List<String> positional = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                String value = i + 1 < args.length ? args[++i] : null;
                if (value == null || value.startsWith("--")) {
                    throw new IllegalArgumentException("选项 " + arg + " 缺少值");
                }
                options.computeIfAbsent(arg, key -> new ArrayList<>()).add(value);
            } else {
                positional.add(arg);
            }
        }
        if (!positional.isEmpty()) {
            throw new IllegalArgumentException("无法识别的参数：" + positional);
        }
        return options;
    }

    private static String requireOption(Map<String, List<String>> options, String name) {
        List<String> values = options.get(name);
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("缺少 " + name);
        }
        return values.get(values.size() - 1);
    }

    private static String single(Map<String, List<String>> options, String name, String fallback) {
        List<String> values = options.get(name);
        return values == null || values.isEmpty() ? fallback : values.get(values.size() - 1);
    }

    private static void usage() {
        System.out.println("""
                kynet-codegen 用法：
                  scan --classpath <目录/jar>... [--include <包前缀>]... --out actions.json
                  gen  --ir actions.json --lang proto|csharp|typescript|gdscript [--lang ...] \\
                       --out <目录> [--namespace Kynet.Client.Generated] [--proto-package kynet.gen] \\
                       [--ts-import-prefix ..] [--gd-addon-path res://addons/kynet] [--gd-generated-path res://addons/kynet/generated]
                  all  --classpath ... --lang ... --out ...
                """);
    }
}
