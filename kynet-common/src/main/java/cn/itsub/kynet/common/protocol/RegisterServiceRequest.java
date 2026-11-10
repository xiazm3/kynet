package cn.itsub.kynet.common.protocol;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class RegisterServiceRequest {
    @Protobuf(fieldType = FieldType.STRING, order = 1)
    private String serviceName;

    @Protobuf(fieldType = FieldType.INT32, order = 2)
    private int serviceId;

    @Protobuf(fieldType = FieldType.INT32, order = 3)
    private int instanceId;

    @Protobuf(fieldType = FieldType.STRING, order = 4)
    private String replyChannel;

    @Protobuf(fieldType = FieldType.INT32, order = 5)
    private int replyStreamId;

    @Protobuf(fieldType = FieldType.INT64, order = 6)
    private long instanceGeneration;

    @Protobuf(fieldType = FieldType.STRING, order = 7)
    private String directChannel;
    @Protobuf(fieldType = FieldType.INT32, order = 8)
    private int directStreamId;
    @Protobuf(fieldType = FieldType.INT64, order = 9)
    private long incarnation;

    public String getDirectChannel() { return directChannel; }
    public void setDirectChannel(String value) { directChannel = value; }
    public int getDirectStreamId() { return directStreamId; }
    public void setDirectStreamId(int value) { directStreamId = value; }
    public long getIncarnation() { return incarnation; }
    public void setIncarnation(long value) { incarnation = value; }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public int getServiceId() {
        return serviceId;
    }

    public void setServiceId(int serviceId) {
        this.serviceId = serviceId;
    }

    public int getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(int instanceId) {
        this.instanceId = instanceId;
    }

    public String getReplyChannel() {
        return replyChannel;
    }

    public void setReplyChannel(String replyChannel) {
        this.replyChannel = replyChannel;
    }

    public int getReplyStreamId() {
        return replyStreamId;
    }

    public void setReplyStreamId(int replyStreamId) {
        this.replyStreamId = replyStreamId;
    }

    public long getInstanceGeneration() {
        return instanceGeneration;
    }

    public void setInstanceGeneration(long instanceGeneration) {
        this.instanceGeneration = instanceGeneration;
    }
}
