package cn.itsub.kynet.codegen.scan;

import cn.itsub.kynet.codegen.ir.EnumMeta;
import cn.itsub.kynet.codegen.ir.FieldMeta;
import cn.itsub.kynet.codegen.ir.MessageMeta;
import cn.itsub.kynet.codegen.ir.TypeKind;
import cn.itsub.kynet.codegen.ir.TypeRef;
import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 把 Java 类型解析成 IR TypeRef，并递归展开 @ProtobufClass 消息图。
 * 规则复刻 jprotobuf 2.4.23 实测行为：
 *  - @Protobuf.order > 0 直接作为字段号；order 缺省(0)时按 max(显式 order)+1 起依声明顺序递增，全缺省从 1 开始。
 *  - @Protobuf.fieldType 缺省(DEFAULT)时按 Java 类型推断。
 * 解析错误集中收集，最后一次性报告。
 */
public final class TypeResolver {
    private final Map<String, MessageMeta> messages = new LinkedHashMap<>();
    private final Map<String, EnumMeta> enums = new LinkedHashMap<>();
    private final List<String> errors = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final Set<String> expanding = new LinkedHashSet<>();

    public List<MessageMeta> messages() {
        return List.copyOf(messages.values());
    }

    public List<EnumMeta> enums() {
        return List.copyOf(enums.values());
    }

    public List<String> errors() {
        return List.copyOf(errors);
    }

    public List<String> warnings() {
        return List.copyOf(warnings);
    }

    /** action 参数/返回值级别的解析：不允许枚举与嵌套 List。 */
    public TypeRef resolveActionType(Type type, String where) {
        // ActionResult 的线上载荷是 getData() 的运行时值，静态反射推不出类型：
        // 按无数据响应生成并提示改用直接返回值。
        Class<?> raw = rawClass(type);
        if (raw != null && "cn.itsub.kynet.framework.action.ActionResult".equals(raw.getName())) {
            warnings.add(where + " 返回 ActionResult，载荷类型无法静态推断，客户端按无数据响应生成；"
                    + "建议改为直接返回 @ProtobufClass 消息或基础类型以获得类型化客户端");
            return null;
        }
        TypeRef ref = resolve(type, where, false);
        if (ref != null && ref.repeated()) {
            TypeRef element = ref.element();
            if (element != null && element.repeated()) {
                error(where, "嵌套 List 不支持");
            }
        }
        return ref;
    }

    private TypeRef resolve(Type type, String where, boolean messageField) {
        Class<?> raw = rawClass(type);
        if (raw == null) {
            error(where, "不支持的泛型类型 " + type);
            return null;
        }
        if (type instanceof ParameterizedType parameterized) {
            Type[] args = parameterized.getActualTypeArguments();
            if (raw == List.class) {
                if (args.length != 1) {
                    error(where, "List 泛型参数缺失");
                    return null;
                }
                TypeRef element = resolve(args[0], where, messageField);
                return element == null ? null : TypeRef.listOf(element);
            }
            error(where, "不支持的泛型容器 " + raw.getSimpleName());
            return null;
        }
        if (raw.isArray()) {
            if (raw == byte[].class) {
                return TypeRef.scalar(TypeKind.BYTES);
            }
            TypeRef element = resolve(raw.getComponentType(), where, messageField);
            return element == null ? null : TypeRef.listOf(element);
        }
        if (raw.isEnum()) {
            if (!messageField) {
                error(where, "枚举不能直接作为 action 参数/返回值，请用 @ProtobufClass 消息包装（"
                        + raw.getName() + "）");
                return null;
            }
            registerEnum(raw);
            return TypeRef.enumRef(raw.getName());
        }
        TypeKind scalar = scalarKind(raw);
        if (scalar != null) {
            return TypeRef.scalar(scalar);
        }
        if (raw.isAnnotationPresent(ProtobufClass.class)) {
            expandMessage(raw, where);
            return TypeRef.message(raw.getName());
        }
        if (List.class.isAssignableFrom(raw)) {
            error(where, "未指定元素类型的 List 不支持（" + raw.getName() + "）");
            return null;
        }
        if (Map.class.isAssignableFrom(raw) || raw == Date.class
                || raw == BigDecimal.class || raw == BigInteger.class) {
            error(where, "kynet-codegen 暂不支持 " + raw.getSimpleName()
                    + (messageField ? " 字段" : " 载荷") + "，请用 @ProtobufClass 消息表达");
            return null;
        }
        error(where, "类型 " + raw.getName() + " 缺少 @ProtobufClass，无法通过 jprotobuf 序列化");
        return null;
    }

    /** 消息字段解析（允许枚举，禁止 google wrapper）。 */
    FieldMeta resolveField(Class<?> declaring, Field field) {
        String where = declaring.getSimpleName() + "." + field.getName();
        Protobuf annotation = field.getAnnotation(Protobuf.class);
        TypeRef ref = resolve(field.getGenericType(), where, true);
        if (ref == null) {
            return null;
        }
        if (annotation != null && annotation.fieldType() != FieldType.DEFAULT) {
            checkDeclaredFieldType(annotation.fieldType(), ref, where);
        }
        return new FieldMeta(field.getName(), 0, ref.repeated(), ref.kind(), ref.message(), ref.enumName());
    }

    /** 按声明顺序补全隐式字段号（复刻 jprotobuf：max(显式)+1 起递增，全缺省从 1 开始）。 */
    void assignOrders(Class<?> declaring, List<FieldMeta> fields) {
        int explicitMax = 0;
        for (Field field : declaring.getDeclaredFields()) {
            Protobuf annotation = field.getAnnotation(Protobuf.class);
            if (annotation != null && annotation.order() > explicitMax) {
                explicitMax = annotation.order();
            }
        }
        Map<String, Integer> explicit = new LinkedHashMap<>();
        for (Field field : declaring.getDeclaredFields()) {
            Protobuf annotation = field.getAnnotation(Protobuf.class);
            if (annotation != null && annotation.order() > 0) {
                explicit.put(field.getName(), annotation.order());
            }
        }
        int implicit = explicitMax;
        for (FieldMeta field : fields) {
            Integer order = explicit.get(field.name());
            if (order != null) {
                fields.set(fields.indexOf(field), new FieldMeta(field.name(), order, field.repeated(),
                        field.kind(), field.message(), field.enumName()));
            } else {
                implicit++;
                fields.set(fields.indexOf(field), new FieldMeta(field.name(), implicit, field.repeated(),
                        field.kind(), field.message(), field.enumName()));
            }
        }
        Set<Integer> seen = new LinkedHashSet<>();
        for (FieldMeta field : fields) {
            if (!seen.add(field.order())) {
                error(declaring.getSimpleName() + "." + field.name(),
                        "字段号冲突：" + field.order() + "（显式 order 重复或与隐式分配冲突）");
            }
        }
    }

    private void checkDeclaredFieldType(FieldType fieldType, TypeRef ref, String where) {
        switch (fieldType) {
            case INT32, INT64, BOOL, DOUBLE, FLOAT, STRING, BYTES, OBJECT, ENUM -> {
                // 支持的 wire 类型；OBJECT 映射 MESSAGE，ENUM 映射 ENUM。
            }
            default -> error(where, "kynet-codegen 暂不支持 @Protobuf(fieldType = FieldType."
                    + fieldType.name() + ")，支持：INT32/INT64/BOOL/DOUBLE/FLOAT/STRING/BYTES/OBJECT/ENUM");
        }
    }

    private void expandMessage(Class<?> type, String where) {
        String key = type.getName();
        if (messages.containsKey(key)) {
            return;
        }
        if (!expanding.add(key)) {
            return;
        }
        Class<?> superclass = type.getSuperclass();
        if (superclass != null && superclass != Object.class) {
            error(where, "消息 " + type.getSimpleName() + " 继承了 " + superclass.getSimpleName()
                    + "，kynet-codegen 不扫描继承字段，请使用扁平消息");
        }
        List<Field> candidates = new ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            int mods = field.getModifiers();
            if (Modifier.isStatic(mods) || Modifier.isTransient(mods) || field.isSynthetic()) {
                continue;
            }
            candidates.add(field);
        }
        MessageMeta meta = new MessageMeta(type.getSimpleName(), key, new ArrayList<>());
        messages.put(key, meta);
        List<FieldMeta> fields = new ArrayList<>();
        for (Field field : candidates) {
            FieldMeta resolved = resolveField(type, field);
            if (resolved != null) {
                fields.add(resolved);
            }
        }
        assignOrders(type, fields);
        meta.fields().addAll(fields);
        expanding.remove(key);
    }

    private void registerEnum(Class<?> type) {
        if (enums.containsKey(type.getName())) {
            return;
        }
        List<EnumMeta.EnumConstant> constants = new ArrayList<>();
        for (Object constant : type.getEnumConstants()) {
            Enum<?> value = (Enum<?>) constant;
            constants.add(new EnumMeta.EnumConstant(value.name(), value.ordinal()));
        }
        enums.put(type.getName(), new EnumMeta(type.getSimpleName(), type.getName(), constants));
    }

    private static TypeKind scalarKind(Class<?> raw) {
        if (raw == String.class) {
            return TypeKind.STRING;
        }
        if (raw == int.class || raw == Integer.class) {
            return TypeKind.INT32;
        }
        if (raw == long.class || raw == Long.class) {
            return TypeKind.INT64;
        }
        if (raw == boolean.class || raw == Boolean.class) {
            return TypeKind.BOOL;
        }
        if (raw == double.class || raw == Double.class) {
            return TypeKind.DOUBLE;
        }
        if (raw == float.class || raw == Float.class) {
            return TypeKind.FLOAT;
        }
        return null;
    }

    private static Class<?> rawClass(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType parameterized) {
            return rawClass(parameterized.getRawType());
        }
        return null;
    }

    private void error(String where, String message) {
        errors.add(where + ": " + message);
    }
}
