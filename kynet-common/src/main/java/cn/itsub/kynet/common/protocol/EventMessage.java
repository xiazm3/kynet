package cn.itsub.kynet.common.protocol;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class EventMessage {
    @Protobuf(fieldType = FieldType.STRING, order = 1)
    private String topic;

    @Protobuf(fieldType = FieldType.BYTES, order = 2)
    private byte[] payload;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
