package cn.itsub.kynet.codegen;

import cn.itsub.kynet.codegen.ir.IrModel;
import cn.itsub.kynet.codegen.render.CSharpRenderer;
import cn.itsub.kynet.codegen.render.GdScriptRenderer;
import cn.itsub.kynet.codegen.render.ProtoRenderer;
import cn.itsub.kynet.codegen.render.RenderOptions;
import cn.itsub.kynet.codegen.render.Renderer;
import cn.itsub.kynet.codegen.render.TypeScriptRenderer;
import cn.itsub.kynet.codegen.scan.ClasspathScanner;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** 命令行与 Java 入口共用的扫描、IR 和渲染流程。 */
final class CodeGenEngine {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    private CodeGenEngine() {
    }

    static IrModel scan(List<Path> classpath, List<String> includes) {
        return new ClasspathScanner().scan(classpath, includes, Instant.now().toString());
    }

    static IrModel readIr(Path path) throws IOException {
        return MAPPER.readValue(path.toFile(), IrModel.class);
    }

    static void writeIr(IrModel model, Path path) throws IOException {
        Path parent = path.getParent();
        Files.createDirectories(parent == null ? Path.of(".") : parent);
        MAPPER.writeValue(path.toFile(), model);
    }

    static List<RenderResult> render(IrModel model, RenderOptions options, Path outDir,
                                     List<String> languages) throws IOException {
        List<RenderResult> results = new ArrayList<>();
        for (String language : languages) {
            List<Path> files = rendererFor(language).render(model, options, outDir);
            results.add(new RenderResult(language, files));
        }
        return results;
    }

    private static Renderer rendererFor(String language) {
        return switch (language) {
            case "proto" -> new ProtoRenderer();
            case "csharp" -> new CSharpRenderer();
            case "typescript", "ts" -> new TypeScriptRenderer();
            case "gdscript", "gd" -> new GdScriptRenderer();
            default -> throw new IllegalArgumentException("未知语言 " + language + "，支持 proto/csharp/typescript/gdscript");
        };
    }

    record RenderResult(String language, List<Path> files) {
    }
}
