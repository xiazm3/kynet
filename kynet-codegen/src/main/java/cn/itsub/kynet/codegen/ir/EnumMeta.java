package cn.itsub.kynet.codegen.ir;

import java.util.List;

/** 枚举元数据；wire 上是 varint 序号（ordinal）。 */
public record EnumMeta(String name, String javaClass, List<EnumConstant> constants) {

    public record EnumConstant(String name, int ordinal) {
    }
}
