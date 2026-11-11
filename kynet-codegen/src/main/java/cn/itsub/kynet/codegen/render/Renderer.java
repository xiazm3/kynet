package cn.itsub.kynet.codegen.render;

import cn.itsub.kynet.codegen.ir.IrModel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/** 一种目标语言的渲染器：把 IR 写成若干源文件。 */
public interface Renderer {
    /** @return 写出的文件（相对 out 目录的路径），供 CLI 汇报。 */
    List<Path> render(IrModel model, RenderOptions options, Path outDir) throws IOException;
}
