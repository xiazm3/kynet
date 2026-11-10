package cn.itsub.kynet.common.protocol;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class RpcResponse {
    @Protobuf(fieldType = FieldType.BOOL, order = 1)
    private boolean success;

    @Protobuf(fieldType = FieldType.STRING, order = 2)
    private String errorCode;

    @Protobuf(fieldType = FieldType.STRING, order = 3)
    private String errorMessage;

    @Protobuf(fieldType = FieldType.STRING, order = 4)
    private String bodyType;

    @Protobuf(fieldType = FieldType.BYTES, order = 5)
    private byte[] body;

    @Protobuf(fieldType = FieldType.BYTES, order = 6)
    private byte[] attachmentData;

    public static RpcResponse ok(String bodyType, byte[] body) {
        return ok(bodyType, body, null);
    }

    public static RpcResponse ok(String bodyType, byte[] body, byte[] attachmentData) {
        RpcResponse response = new RpcResponse();
        response.setSuccess(true);
        response.setBodyType(bodyType);
        response.setBody(body);
        response.setAttachmentData(attachmentData);
        return response;
    }

    public static RpcResponse error(String errorCode, String errorMessage) {
        return error(errorCode, errorMessage, null);
    }

    public static RpcResponse error(String errorCode, String errorMessage, byte[] attachmentData) {
        RpcResponse response = new RpcResponse();
        response.setSuccess(false);
        response.setErrorCode(errorCode);
        response.setErrorMessage(errorMessage);
        response.setAttachmentData(attachmentData);
        return response;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getBodyType() {
        return bodyType;
    }

    public void setBodyType(String bodyType) {
        this.bodyType = bodyType;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte[] getAttachmentData() {
        return attachmentData;
    }

    public void setAttachmentData(byte[] attachmentData) {
        this.attachmentData = attachmentData;
    }
}
