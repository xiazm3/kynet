package cn.itsub.game.rpc.demo.message;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class RpcDemoRequest {
    @Protobuf(fieldType = FieldType.STRING, order = 1)
    private String message;

    public RpcDemoRequest() {
    }

    public RpcDemoRequest(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "RpcDemoRequest{" +
                "message='" + message + '\'' +
                '}';
    }
}
