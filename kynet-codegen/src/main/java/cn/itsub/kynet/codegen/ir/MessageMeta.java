package cn.itsub.kynet.codegen.ir;

import java.util.List;

/** @ProtobufClass 消息元数据；javaClass 是 IR 内的消息唯一键。 */
public record MessageMeta(String name, String javaClass, List<FieldMeta> fields) {
}
