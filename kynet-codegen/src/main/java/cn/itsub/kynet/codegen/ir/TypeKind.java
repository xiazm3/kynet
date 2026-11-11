package cn.itsub.kynet.codegen.ir;

/** 语言无关的载荷叶子类型。MESSAGE/ENUM 需配合 message/enumName 引用。 */
public enum TypeKind {
    /** @ProtobufClass POJO，wire 为嵌套 protobuf message。 */
    MESSAGE,
    /** Java String，action 载荷为裸 UTF-8 字节。 */
    STRING,
    /** Java byte[]，action 载荷为裸字节。 */
    BYTES,
    INT32,
    INT64,
    BOOL,
    DOUBLE,
    FLOAT,
    /** 枚举字段（wire 为 varint 序号），仅允许出现在消息字段中。 */
    ENUM
}
