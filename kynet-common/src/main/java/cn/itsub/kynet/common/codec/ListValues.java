package cn.itsub.kynet.common.codec;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

import java.util.ArrayList;
import java.util.List;

/** 将 List 载荷包装成 {@link ListValue} 信封，元素类型按首个非空元素判定。 */
final class ListValues {
    private ListValues() {
    }

    static ListValue of(List<?> values) {
        ListValue listValue = new ListValue();
        if (values == null || values.isEmpty()) {
            return listValue.setKind(ListValue.Kind.STRING);
        }
        Object first = values.stream().filter(java.util.Objects::nonNull).findFirst()
                .orElse(null);
        if (first == null) {
            return listValue.setKind(ListValue.Kind.STRING);
        }
        if (first instanceof String ignored) {
            List<String> converted = new ArrayList<>(values.size());
            for (Object value : values) {
                converted.add(require(value, String.class, values));
            }
            return listValue.setKind(ListValue.Kind.STRING).setStringValues(converted);
        }
        if (first instanceof Integer ignored) {
            List<Integer> converted = new ArrayList<>(values.size());
            for (Object value : values) {
                converted.add(require(value, Integer.class, values));
            }
            return listValue.setKind(ListValue.Kind.INT).setIntValues(converted);
        }
        if (first instanceof Long ignored) {
            List<Long> converted = new ArrayList<>(values.size());
            for (Object value : values) {
                converted.add(require(value, Long.class, values));
            }
            return listValue.setKind(ListValue.Kind.LONG).setLongValues(converted);
        }
        if (first instanceof Boolean ignored) {
            List<Boolean> converted = new ArrayList<>(values.size());
            for (Object value : values) {
                converted.add(require(value, Boolean.class, values));
            }
            return listValue.setKind(ListValue.Kind.BOOL).setBoolValues(converted);
        }
        if (first instanceof Double ignored) {
            List<Double> converted = new ArrayList<>(values.size());
            for (Object value : values) {
                converted.add(require(value, Double.class, values));
            }
            return listValue.setKind(ListValue.Kind.DOUBLE).setDoubleValues(converted);
        }
        if (first instanceof Float ignored) {
            List<Float> converted = new ArrayList<>(values.size());
            for (Object value : values) {
                converted.add(require(value, Float.class, values));
            }
            return listValue.setKind(ListValue.Kind.FLOAT).setFloatValues(converted);
        }
        if (first.getClass().isAnnotationPresent(ProtobufClass.class)) {
            List<byte[]> converted = new ArrayList<>(values.size());
            for (Object value : values) {
                if (value instanceof List<?> nested) {
                    throw new IllegalStateException("Nested lists are not supported, element type "
                            + first.getClass().getName());
                }
                converted.add(ProtobufPayloadCodec.encode(value));
            }
            return listValue.setKind(ListValue.Kind.MESSAGE)
                    .setMessageValues(converted)
                    .setElementType(first.getClass().getName());
        }
        throw new IllegalStateException("Unsupported list element type " + first.getClass().getName()
                + "; use a @" + ProtobufClass.class.getSimpleName() + " wrapper message");
    }

    private static <T> T require(Object value, Class<T> type, List<?> values) {
        if (value == null) {
            throw new IllegalStateException("Null element in list payload of " + type.getSimpleName());
        }
        if (!type.isInstance(value)) {
            throw new IllegalStateException("Mixed element types in list payload: expected "
                    + type.getSimpleName() + " but found " + value.getClass().getName());
        }
        return type.cast(value);
    }
}
