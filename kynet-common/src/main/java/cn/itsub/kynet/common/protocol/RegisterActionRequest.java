package cn.itsub.kynet.common.protocol;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class RegisterActionRequest {
    @Protobuf(fieldType = FieldType.INT32, order = 1)
    private int action;

    @Protobuf(fieldType = FieldType.INT32, order = 2)
    private int serviceId;

    @Protobuf(fieldType = FieldType.INT32, order = 3)
    private int instanceId;

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
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
}
