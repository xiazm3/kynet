package cn.itsub.kynet.codegen.scan;

import cn.itsub.kynet.codegen.ir.ActionMeta;
import cn.itsub.kynet.codegen.ir.IrModel;
import cn.itsub.kynet.codegen.ir.TypeRef;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.annotation.ActionAnnotations;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/**
 * 扫描 classpath（目录/jar）上的 @ActionController，产出 IrModel。
 * 类以 initialize=false 加载，避免触发静态初始化；注解从工具自身 classpath（kynet-framework/jprotobuf）解析。
 */
public final class ClasspathScanner {
    private final List<String> errors = new ArrayList<>();
    private final TypeResolver resolver = new TypeResolver();

    public IrModel scan(List<Path> classpathEntries, List<String> includes, String generatedAt) {
        if (classpathEntries.isEmpty()) {
            throw new IllegalArgumentException("至少需要一个 --classpath 条目");
        }
        URL[] urls = classpathEntries.stream()
                .map(path -> {
                    try {
                        return path.toUri().toURL();
                    } catch (IOException e) {
                        throw new IllegalArgumentException("非法 classpath 路径 " + path, e);
                    }
                })
                .toArray(URL[]::new);
        List<ActionMeta> actions = new ArrayList<>();
        try (URLClassLoader loader = new URLClassLoader(urls, ClasspathScanner.class.getClassLoader())) {
            for (String name : collectClassNames(classpathEntries, includes)) {
                Class<?> type;
                try {
                    type = Class.forName(name, false, loader);
                } catch (Throwable ignored) {
                    // 依赖缺失的类无法判断注解，直接跳过；确保 --classpath 覆盖完整模块。
                    continue;
                }
                if (type.isAnnotationPresent(ActionController.class)) {
                    collectController(type, actions);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("扫描 classpath 失败", e);
        }

        errors.addAll(resolver.errors());
        checkSimpleNameCollisions(resolver);
        finishValidation(actions);
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("采集校验失败：\n  - " + String.join("\n  - ", errors));
        }
        actions.sort(Comparator.comparingInt(ActionMeta::cmd).thenComparingInt(ActionMeta::act));
        return new IrModel(IrModel.FORMAT_VERSION, generatedAt, actions, resolver.messages(),
                resolver.enums(), resolver.warnings());
    }

    private void collectController(Class<?> type, List<ActionMeta> actions) {
        ActionController controller = type.getAnnotation(ActionController.class);
        int cmd;
        try {
            cmd = ActionAnnotations.cmd(controller);
        } catch (IllegalArgumentException e) {
            errors.add(type.getSimpleName() + ": " + e.getMessage());
            return;
        }
        if (cmd > 65535) {
            errors.add(type.getSimpleName() + ": cmd 必须在 1..65535，当前 " + cmd);
            return;
        }
        List<Method> methods = new ArrayList<>();
        for (Method method : type.getDeclaredMethods()) {
            if (method.isAnnotationPresent(ActionMethod.class)) {
                methods.add(method);
            }
        }
        // 排序仅用原始 value（编译期必填，反射读取不会失败；非法值由后续逐项收集时报错）
        methods.sort(Comparator.comparingInt(method -> method.getAnnotation(ActionMethod.class).value()));
        Set<Integer> acts = new HashSet<>();
        for (Method method : methods) {
            ActionMethod annotation = method.getAnnotation(ActionMethod.class);
            int act;
            try {
                act = ActionAnnotations.act(annotation);
            } catch (IllegalArgumentException e) {
                errors.add(type.getSimpleName() + "." + method.getName() + ": " + e.getMessage());
                continue;
            }
            if (act > 65535) {
                errors.add(type.getSimpleName() + "." + method.getName() + ": act 必须在 1..65535，当前 " + act);
                continue;
            }
            if (!acts.add(act)) {
                errors.add(type.getSimpleName() + "." + method.getName() + ": act=" + act + " 重复");
                continue;
            }
            actions.add(buildAction(cmd, act, type, method, annotation));
        }
    }

    private ActionMeta buildAction(int cmd, int act, Class<?> type, Method method, ActionMethod annotation) {
        String where = type.getSimpleName() + "." + method.getName();
        Type[] genericParams = method.getGenericParameterTypes();
        Parameter[] parameters = method.getParameters();
        TypeRef paramType = null;
        String paramName = null;
        boolean needsContext = false;
        int business = 0;
        for (int i = 0; i < genericParams.length; i++) {
            if (genericParams[i] == ActionContext.class) {
                needsContext = true;
                continue;
            }
            business++;
            if (business == 1) {
                paramType = resolver.resolveActionType(genericParams[i], where + " 参数");
                paramName = parameterName(parameters[i].getName(), genericParams[i]);
            } else {
                errors.add(where + ": action 最多一个业务载荷参数（其余须为 ActionContext）");
            }
        }
        TypeRef returnType = null;
        if (method.getReturnType() != void.class) {
            returnType = resolver.resolveActionType(method.getGenericReturnType(), where + " 返回值");
        }
        String executionMode = annotation.mode() == null ? "ASYNC" : annotation.mode().name();
        return new ActionMeta(cmd, act, type.getName(), type.getSimpleName(), method.getName(),
                executionMode, paramType, paramName, returnType, needsContext);
    }

    private String parameterName(String declared, Type type) {
        if (declared == null || declared.matches("arg\\d+")) {
            String simple = simpleName(type);
            return Character.toLowerCase(simple.charAt(0)) + simple.substring(1);
        }
        return declared;
    }

    private String simpleName(Type type) {
        String name = type.getTypeName();
        int generic = name.indexOf('<');
        if (generic > 0) {
            name = name.substring(0, generic);
        }
        int dot = name.lastIndexOf('.');
        name = dot > 0 ? name.substring(dot + 1) : name;
        int dollar = name.indexOf('$');
        return dollar > 0 ? name.substring(dollar + 1) : name;
    }

    /** 各语言渲染器都以 simple name 生成类型，重名无法区分。 */
    private void checkSimpleNameCollisions(TypeResolver resolver) {
        Map<String, String> seenMessages = new LinkedHashMap<>();
        for (var message : resolver.messages()) {
            String previous = seenMessages.putIfAbsent(message.name(), message.javaClass());
            if (previous != null) {
                errors.add("消息 simple name 冲突：" + previous + " 与 " + message.javaClass()
                        + " 都叫 " + message.name() + "，生成器无法区分");
            }
        }
        Map<String, String> seenEnums = new LinkedHashMap<>();
        for (var enumMeta : resolver.enums()) {
            String previous = seenEnums.putIfAbsent(enumMeta.name(), enumMeta.javaClass());
            if (previous != null) {
                errors.add("枚举 simple name 冲突：" + previous + " 与 " + enumMeta.javaClass()
                        + " 都叫 " + enumMeta.name());
            }
        }
    }

    private void finishValidation(List<ActionMeta> actions) {
        Map<String, ActionMeta> seen = new LinkedHashMap<>();
        for (ActionMeta action : actions) {
            String key = action.cmd() + ":" + action.act();
            ActionMeta previous = seen.putIfAbsent(key, action);
            if (previous != null) {
                errors.add("action " + key + " 冲突：" + previous.controllerClass() + "."
                        + previous.methodName() + " 与 " + action.controllerClass() + "." + action.methodName());
            }
        }
        if (actions.isEmpty()) {
            errors.add("classpath 上没有找到任何 @ActionController，请确认 --classpath 指向编译产物");
        }
        Set<String> rpcNames = new HashSet<>();
        for (ActionMeta action : actions) {
            String rpc = pascal(action.methodName());
            if (!rpcNames.add(action.controllerName() + "." + rpc)) {
                errors.add(action.controllerName() + " 内方法 " + action.methodName()
                        + " 的 PascalCase 名称与其它方法冲突（" + rpc + "），生成器无法区分");
            }
        }
    }

    private String pascal(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    private List<String> collectClassNames(List<Path> entries, List<String> includes) throws IOException {
        Set<String> names = new HashSet<>();
        for (Path entry : entries) {
            if (Files.isDirectory(entry)) {
                try (Stream<Path> files = Files.walk(entry)) {
                    files.filter(path -> path.toString().endsWith(".class")).forEach(path -> {
                        String fqcn = entry.relativize(path).toString()
                                .replace('\\', '.').replace('/', '.');
                        fqcn = fqcn.substring(0, fqcn.length() - ".class".length());
                        if (!fqcn.contains("module-info") && !fqcn.contains("package-info")) {
                            names.add(fqcn);
                        }
                    });
                }
            } else if (Files.isRegularFile(entry)) {
                try (JarFile jar = new JarFile(entry.toFile())) {
                    Enumeration<JarEntry> jarEntries = jar.entries();
                    while (jarEntries.hasMoreElements()) {
                        JarEntry jarEntry = jarEntries.nextElement();
                        String name = jarEntry.getName();
                        if (name.endsWith(".class") && !name.contains("module-info")
                                && !name.contains("package-info")) {
                            names.add(name.substring(0, name.length() - ".class".length())
                                    .replace('/', '.'));
                        }
                    }
                }
            } else {
                errors.add("classpath 条目不存在：" + entry);
            }
        }
        if (includes == null || includes.isEmpty()) {
            return new ArrayList<>(names);
        }
        List<String> filtered = new ArrayList<>();
        for (String name : names) {
            for (String prefix : includes) {
                if (name.startsWith(prefix)) {
                    filtered.add(name);
                    break;
                }
            }
        }
        return filtered;
    }

    /** 供 CLI 读取错误列表（成功时为空）。 */
    public List<String> errors() {
        return List.copyOf(errors);
    }
}
