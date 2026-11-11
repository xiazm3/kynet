package cn.itsub.kynet.codegen;

import cn.itsub.kynet.codegen.ir.IrModel;
import cn.itsub.kynet.codegen.render.RenderOptions;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** 在服务端项目中直接调用的代码生成入口；默认扫描运行时 classpath 并生成 C# 客户端。 */
public final class CodeGenHelper {
    private CodeGenHelper() {
    }

    /** 使用默认项目、输出目录和 C# 目标生成代码。 */
    public static List<Path> generate() throws IOException {
        return configure().generate();
    }

    /** 创建一次性的配置；不会修改其他调用的默认值。 */
    public static Configuration configure() {
        Class<?> caller = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)
                .walk(frames -> frames.map(StackWalker.StackFrame::getDeclaringClass)
                        .filter(type -> type != CodeGenHelper.class)
                        .findFirst().orElseThrow());
        return new Configuration(caller);
    }

    private static Path codeSource(Class<?> type) {
        try {
            if (type.getProtectionDomain().getCodeSource() == null) {
                return null;
            }
            return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                    .toAbsolutePath().normalize();
        } catch (URISyntaxException | SecurityException e) {
            return null;
        }
    }

    private static Path projectRoot(Class<?> caller) {
        Path location = codeSource(caller);
        if (location != null) {
            Path parent = location.getParent();
            if (parent != null && parent.getFileName() != null && "target".equals(parent.getFileName().toString())) {
                return parent.getParent();
            }
            if (Files.isDirectory(location) && parent != null && "java".equals(parent.getFileName().toString())) {
                Path classes = parent.getParent();
                if (classes != null && "classes".equals(classes.getFileName().toString())) {
                    Path build = classes.getParent();
                    if (build != null && "build".equals(build.getFileName().toString())) {
                        return build.getParent();
                    }
                }
            }
        }
        return Path.of("").toAbsolutePath().normalize();
    }

    public static final class Configuration {
        private final Class<?> caller;
        private Path projectRoot;
        private Path outputDirectory;
        private Path irSource;
        private List<Path> classpathEntries;
        private List<String> includePackages = List.of();
        private List<ClientTarget> targets = List.of(ClientTarget.CSHARP);
        private String csharpNamespace = "Kynet.Client.Generated";
        private String protoPackage = "kynet.gen";
        private String tsImportPrefix = "..";
        private String gdAddonPath = "res://addons/kynet";
        private String gdGeneratedPath = "res://addons/kynet/generated";

        private Configuration(Class<?> caller) {
            this.caller = caller;
            this.projectRoot = CodeGenHelper.projectRoot(caller);
        }

        /** 覆盖自动识别的项目目录；相对路径按当前进程目录解析。 */
        public Configuration projectRoot(Path root) {
            this.projectRoot = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
            return this;
        }

        /** 不指定时输出到项目根目录下的 generated/kynet。 */
        public Configuration outputDirectory(Path directory) {
            this.outputDirectory = Objects.requireNonNull(directory, "directory");
            return this;
        }

        public Configuration target(ClientTarget target) {
            return targets(target);
        }

        /** 选择一个或多个目标；替换默认的 C# 目标。 */
        public Configuration targets(ClientTarget... targets) {
            Objects.requireNonNull(targets, "targets");
            if (targets.length == 0) {
                throw new IllegalArgumentException("至少选择一个生成目标");
            }
            this.targets = List.copyOf(new LinkedHashSet<>(Arrays.asList(targets)));
            return this;
        }

        /** 覆盖运行时 classpath 自动发现；依赖由运行时 classloader 提供。 */
        public Configuration classpathEntries(Path... entries) {
            Objects.requireNonNull(entries, "entries");
            if (entries.length == 0) {
                throw new IllegalArgumentException("classpathEntries 不能为空");
            }
            this.classpathEntries = List.copyOf(Arrays.asList(entries));
            return this;
        }

        /** 可选的包前缀过滤；默认扫描运行时可见的所有 Action。 */
        public Configuration includePackages(String... prefixes) {
            Objects.requireNonNull(prefixes, "prefixes");
            this.includePackages = List.copyOf(Arrays.asList(prefixes));
            return this;
        }

        public Configuration csharpNamespace(String namespace) {
            this.csharpNamespace = nonBlank(namespace, "csharpNamespace");
            return this;
        }

        public Configuration protoPackage(String packageName) {
            this.protoPackage = nonBlank(packageName, "protoPackage");
            return this;
        }

        public Configuration tsImportPrefix(String prefix) {
            this.tsImportPrefix = nonBlank(prefix, "tsImportPrefix");
            return this;
        }

        public Configuration gdAddonPath(String path) {
            this.gdAddonPath = nonBlank(path, "gdAddonPath");
            return this;
        }

        public Configuration gdGeneratedPath(String path) {
            this.gdGeneratedPath = nonBlank(path, "gdGeneratedPath");
            return this;
        }

        /** 从已有 actions.json 渲染，等价于 CLI 的 gen 模式。 */
        public Configuration fromIr(Path path) {
            this.irSource = Objects.requireNonNull(path, "path");
            return this;
        }

        /** 扫描并返回 IR，等价于 CLI 的 scan 模式但不写文件。 */
        public IrModel scan() {
            return CodeGenEngine.scan(resolveClasspath(), includePackages);
        }

        /** 扫描并保存 actions.json。 */
        public Path scanTo(Path path) throws IOException {
            Path output = resolve(Objects.requireNonNull(path, "path"));
            CodeGenEngine.writeIr(scan(), output);
            return output;
        }

        /** 扫描并生成，或从 fromIr 指定的 IR 生成；返回全部输出文件。 */
        public List<Path> generate() throws IOException {
            IrModel model = irSource == null ? scan() : CodeGenEngine.readIr(resolve(irSource));
            RenderOptions options = new RenderOptions(csharpNamespace, protoPackage, tsImportPrefix,
                    gdAddonPath, gdGeneratedPath, Instant.now().toString());
            Path output = outputDirectory == null ? projectRoot.resolve("generated/kynet") : resolve(outputDirectory);
            List<String> languages = targets.stream().map(ClientTarget::language).toList();
            List<Path> files = new ArrayList<>();
            for (CodeGenEngine.RenderResult result : CodeGenEngine.render(model, options, output, languages)) {
                files.addAll(result.files());
            }
            return List.copyOf(files);
        }

        private List<Path> resolveClasspath() {
            if (classpathEntries != null) {
                return classpathEntries.stream().map(this::resolve).toList();
            }
            Set<Path> found = new LinkedHashSet<>();
            String javaClasspath = System.getProperty("java.class.path", "");
            for (String entry : javaClasspath.split(Pattern.quote(File.pathSeparator))) {
                if (!entry.isBlank()) {
                    addClasspathEntry(found, Path.of(entry).toAbsolutePath().normalize());
                }
            }
            for (ClassLoader loader = Thread.currentThread().getContextClassLoader();
                 loader != null; loader = loader.getParent()) {
                if (loader instanceof URLClassLoader urlLoader) {
                    for (URL url : urlLoader.getURLs()) {
                        if ("file".equals(url.getProtocol())) {
                            try {
                                addClasspathEntry(found, Path.of(url.toURI()).toAbsolutePath().normalize());
                            } catch (URISyntaxException | IllegalArgumentException ignored) {
                                // 非本地 classpath URL 不适用于现有的文件扫描器。
                            }
                        }
                    }
                }
            }
            Path callerLocation = codeSource(caller);
            if (callerLocation != null) {
                addClasspathEntry(found, callerLocation);
            }
            addClasspathEntry(found, projectRoot.resolve("target/classes"));
            addClasspathEntry(found, projectRoot.resolve("build/classes/java/main"));
            if (found.isEmpty()) {
                throw new IllegalStateException("当前项目没有可扫描的编译产物；请先编译，或配置 classpathEntries");
            }
            return List.copyOf(found);
        }

        private void addClasspathEntry(Set<Path> found, Path entry) {
            if (Files.isDirectory(entry) || Files.isRegularFile(entry) && entry.toString().endsWith(".jar")) {
                found.add(entry);
            }
        }

        private Path resolve(Path path) {
            return (path.isAbsolute() ? path : projectRoot.resolve(path)).toAbsolutePath().normalize();
        }

        private static String nonBlank(String value, String name) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(name + " 不能为空");
            }
            return value;
        }
    }
}
