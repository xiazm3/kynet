package cn.itsub.kynet.common.codec;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class PrimitiveValue {
    public enum Kind {
        INT,
        LONG,
        BOOL,
        DOUBLE,
        FLOAT
    }

    @Protobuf(order = 1, required = true)
    private Kind valueKind;

    @Protobuf(fieldType = FieldType.INT32, order = 2)
    private Integer intValue;

    @Protobuf(fieldType = FieldType.INT64, order = 3)
    private Long longValue;

    @Protobuf(fieldType = FieldType.BOOL, order = 4)
    private Boolean boolValue;

    @Protobuf(fieldType = FieldType.DOUBLE, order = 5)
    private Double doubleValue;

    @Protobuf(fieldType = FieldType.FLOAT, order = 6)
    private Float floatValue;

    public Kind getValueKind() {
        return valueKind;
    }

    public PrimitiveValue setValueKind(Kind valueKind) {
        this.valueKind = valueKind;
        return this;
    }

    public Integer getIntValue() {
        return intValue;
    }

    public PrimitiveValue setIntValue(Integer intValue) {
        this.intValue = intValue;
        return this;
    }

    public Long getLongValue() {
        return longValue;
    }

    public PrimitiveValue setLongValue(Long longValue) {
        this.longValue = longValue;
        return this;
    }

    public Boolean getBoolValue() {
        return boolValue;
    }

    public PrimitiveValue setBoolValue(Boolean boolValue) {
        this.boolValue = boolValue;
        return this;
    }

    public Double getDoubleValue() {
        return doubleValue;
    }

    public PrimitiveValue setDoubleValue(Double doubleValue) {
        this.doubleValue = doubleValue;
        return this;
    }

    public Float getFloatValue() {
        return floatValue;
    }

    public PrimitiveValue setFloatValue(Float floatValue) {
        this.floatValue = floatValue;
        return this;
    }
}
