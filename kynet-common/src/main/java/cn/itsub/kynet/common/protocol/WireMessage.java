package cn.itsub.kynet.common.protocol;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class WireMessage {
    @Protobuf(order = 1, required = true)
    private MessageType messageType;

    @Protobuf(fieldType = FieldType.INT64, order = 2)
    private long requestId;

    @Protobuf(fieldType = FieldType.STRING, order = 3)
    private String traceId;

    @Protobuf(fieldType = FieldType.INT32, order = 4)
    private int action;

    @Protobuf(fieldType = FieldType.INT32, order = 5)
    private int sourceServiceId;

    @Protobuf(fieldType = FieldType.INT32, order = 6)
    private int sourceInstanceId;

    @Protobuf(fieldType = FieldType.INT32, order = 7)
    private int targetServiceId;

    @Protobuf(fieldType = FieldType.INT32, order = 8)
    private int targetInstanceId;

    @Protobuf(fieldType = FieldType.STRING, order = 9)
    private String topic;

    @Protobuf(fieldType = FieldType.STRING, order = 10)
    private String payloadType;

    @Protobuf(fieldType = FieldType.BYTES, order = 11)
    private byte[] payload;

    private int topicHash;

    public MessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(MessageType messageType) {
        this.messageType = messageType;
    }

    public long getRequestId() {
        return requestId;
    }

    public void setRequestId(long requestId) {
        this.requestId = requestId;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public int getSourceServiceId() {
        return sourceServiceId;
    }

    public void setSourceServiceId(int sourceServiceId) {
        this.sourceServiceId = sourceServiceId;
    }

    public int getSourceInstanceId() {
        return sourceInstanceId;
    }

    public void setSourceInstanceId(int sourceInstanceId) {
        this.sourceInstanceId = sourceInstanceId;
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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
        this.topicHash = topic == null ? 0 : topic.hashCode();
    }

    public int getTopicHash() {
        return topicHash;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
