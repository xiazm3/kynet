package cn.itsub.kynet.codegen.ir;

import com.fasterxml.jackson.annotation.JsonInclude;

/** 单条 action：cmd/act 是路由键，paramType/returnType 描述线上载荷。 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ActionMeta(
        int cmd,
        int act,
        String controllerClass,
        String controllerName,
        String methodName,
        String executionMode,
        TypeRef paramType,
        String paramName,
        TypeRef returnType,
        boolean needsContext) {
}
