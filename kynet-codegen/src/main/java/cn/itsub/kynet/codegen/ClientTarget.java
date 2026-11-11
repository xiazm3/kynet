package cn.itsub.kynet.codegen;

/** CodeGenHelper 可生成的目标；PROTO 是契约文件而非客户端语言。 */
public enum ClientTarget {
    CSHARP("csharp"),
    TYPESCRIPT("typescript"),
    GDSCRIPT("gdscript"),
    PROTO("proto");

    private final String language;

    ClientTarget(String language) {
        this.language = language;
    }

    String language() {
        return language;
    }
}
