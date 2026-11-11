package cn.itsub.kynet.codegen.render;

import cn.itsub.kynet.codegen.FixtureSupport;
import cn.itsub.kynet.codegen.ir.IrModel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** 渲染器测试共享：渲染到临时目录并返回输出目录。 */
public final class RendererTestBase {
    private RendererTestBase() {
    }

    public static Path renderToTemp(Renderer renderer, String lang) throws IOException {
        IrModel model = FixtureSupport.scanFixtures();
        RenderOptions options = RenderOptions.defaults(model.generatedAt());
        Path out = Files.createTempDirectory("kynet-codegen-" + lang);
        renderer.render(model, options, out);
        return out;
    }

    public static String readAll(Path dir, String fileName) throws IOException {
        return Files.readString(dir.resolve(fileName));
    }
}
