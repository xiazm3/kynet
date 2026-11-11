package cn.itsub.kynet.codegen.ir;

import com.fasterxml.jackson.annotation.JsonInclude;

/** action 参数/返回值类型引用；repeated=true 时 element 描述元素类型。 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TypeRef(TypeKind kind, String message, String enumName, boolean repeated, TypeRef element) {

    public static TypeRef scalar(TypeKind kind) {
        return new TypeRef(kind, null, null, false, null);
    }

    public static TypeRef message(String javaClass) {
        return new TypeRef(TypeKind.MESSAGE, javaClass, null, false, null);
    }

    public static TypeRef enumRef(String javaClass) {
        return new TypeRef(TypeKind.ENUM, null, javaClass, false, null);
    }

    public static TypeRef listOf(TypeRef element) {
        return new TypeRef(element.kind(), element.message(), element.enumName(), true, element);
    }
}
