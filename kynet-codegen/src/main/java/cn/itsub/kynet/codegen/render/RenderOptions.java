package cn.itsub.kynet.codegen.render;

/** 渲染选项；各语言渲染器按需读取。 */
public record RenderOptions(
        String csharpNamespace,
        String protoPackage,
        String tsImportPrefix,
        String gdAddonPath,
        String gdGeneratedPath,
        String generatedAt) {

    public static RenderOptions defaults(String generatedAt) {
        return new RenderOptions("Kynet.Client.Generated", "kynet.gen",
                "..", "res://addons/kynet", "res://addons/kynet/generated", generatedAt);
    }
}
