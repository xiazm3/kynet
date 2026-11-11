package cn.itsub.kynet.common.codec;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

import java.util.List;

/**
 * kynet List&lt;T&gt; action 载荷包装。List 不能直接走 jprotobuf 编码，
 * 服务端把 List 包成该信封发送，客户端生成代码按 kind 解包。
 * 字段号是跨语言 wire 契约，变更需同步所有客户端生成器。
 */
@ProtobufClass
public class ListValue {
    public enum Kind {
        STRING,
        INT,
        LONG,
        BOOL,
        DOUBLE,
        FLOAT,
        MESSAGE
    }

    @Protobuf(order = 1, required = true)
    private Kind kind;

    @Protobuf(fieldType = FieldType.STRING, order = 2)
    private List<String> stringValues;

    @Protobuf(fieldType = FieldType.INT32, order = 3)
    private List<Integer> intValues;

    @Protobuf(fieldType = FieldType.INT64, order = 4)
    private List<Long> longValues;

    @Protobuf(fieldType = FieldType.BOOL, order = 5)
    private List<Boolean> boolValues;

    @Protobuf(fieldType = FieldType.DOUBLE, order = 6)
    private List<Double> doubleValues;

    @Protobuf(fieldType = FieldType.FLOAT, order = 7)
    private List<Float> floatValues;

    /** MESSAGE 元素的逐条编码结果，客户端按元素类型解码。 */
    @Protobuf(fieldType = FieldType.BYTES, order = 8)
    private List<byte[]> messageValues;

    /** MESSAGE 元素的 Java 类全名，仅用于诊断。 */
    @Protobuf(fieldType = FieldType.STRING, order = 9)
    private String elementType;

    public Kind getKind() {
        return kind;
    }

    public ListValue setKind(Kind kind) {
        this.kind = kind;
        return this;
    }

    public List<String> getStringValues() {
        return stringValues;
    }

    public ListValue setStringValues(List<String> stringValues) {
        this.stringValues = stringValues;
        return this;
    }

    public List<Integer> getIntValues() {
        return intValues;
    }

    public ListValue setIntValues(List<Integer> intValues) {
        this.intValues = intValues;
        return this;
    }

    public List<Long> getLongValues() {
        return longValues;
    }

    public ListValue setLongValues(List<Long> longValues) {
        this.longValues = longValues;
        return this;
    }

    public List<Boolean> getBoolValues() {
        return boolValues;
    }

    public ListValue setBoolValues(List<Boolean> boolValues) {
        this.boolValues = boolValues;
        return this;
    }

    public List<Double> getDoubleValues() {
        return doubleValues;
    }

    public ListValue setDoubleValues(List<Double> doubleValues) {
        this.doubleValues = doubleValues;
        return this;
    }

    public List<Float> getFloatValues() {
        return floatValues;
    }

    public ListValue setFloatValues(List<Float> floatValues) {
        this.floatValues = floatValues;
        return this;
    }

    public List<byte[]> getMessageValues() {
        return messageValues;
    }

    public ListValue setMessageValues(List<byte[]> messageValues) {
        this.messageValues = messageValues;
        return this;
    }

    public String getElementType() {
        return elementType;
    }

    public ListValue setElementType(String elementType) {
        this.elementType = elementType;
        return this;
    }
}
