package cn.itsub.kynet.codegen.ir;

import java.util.List;

/** 采集器输出的语言无关中间表示（actions.json）。渲染器只消费本契约。 */
public record IrModel(
        int formatVersion,
        String generatedAt,
        List<ActionMeta> actions,
        List<MessageMeta> messages,
        List<EnumMeta> enums,
        List<String> warnings) {

    public static final int FORMAT_VERSION = 1;
}
