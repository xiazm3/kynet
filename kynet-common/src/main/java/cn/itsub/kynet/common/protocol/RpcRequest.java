package cn.itsub.kynet.common.protocol;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class RpcRequest {
    @Protobuf(fieldType = FieldType.INT32, order = 1)
    private int action;

    @Protobuf(fieldType = FieldType.STRING, order = 2)
    private String bodyType;

    @Protobuf(fieldType = FieldType.BYTES, order = 3)
    private byte[] body;

    @Protobuf(fieldType = FieldType.INT64, order = 4)
    private long sessionId;

    @Protobuf(fieldType = FieldType.INT64, order = 5)
    private long userId;

    @Protobuf(fieldType = FieldType.INT64, order = 6)
    private long timeoutMs;

    @Protobuf(fieldType = FieldType.INT32, order = 7)
    private int targetServiceId;

    @Protobuf(fieldType = FieldType.INT32, order = 8)
    private int targetInstanceId;

    @Protobuf(fieldType = FieldType.BOOL, order = 9)
    private boolean oneWay;

    @Protobuf(fieldType = FieldType.BYTES, order = 10)
    private byte[] attachmentData;

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
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

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public int getTargetServiceId() {
        return targetServiceId;
    }

    public void setTargetServiceId(int targetServiceId) {
        this.targetServiceId = targetServiceId;
    }

    public int getTargetInstanceId() {
        return targetInstanceId;
    }

    public void setTargetInstanceId(int targetInstanceId) {
        this.targetInstanceId = targetInstanceId;
    }

    public boolean isOneWay() {
        return oneWay;
    }

    public void setOneWay(boolean oneWay) {
        this.oneWay = oneWay;
    }

    public byte[] getAttachmentData() {
        return attachmentData;
    }

    public void setAttachmentData(byte[] attachmentData) {
        this.attachmentData = attachmentData;
    }
}
