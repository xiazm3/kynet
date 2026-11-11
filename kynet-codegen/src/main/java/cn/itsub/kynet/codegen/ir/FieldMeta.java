package cn.itsub.kynet.codegen.ir;

import com.fasterxml.jackson.annotation.JsonInclude;

/** 消息字段元数据；order 即 protobuf 字段号（复刻 jprotobuf 显式/隐式规则后得出）。 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FieldMeta(String name, int order, boolean repeated, TypeKind kind, String message, String enumName) {
}
