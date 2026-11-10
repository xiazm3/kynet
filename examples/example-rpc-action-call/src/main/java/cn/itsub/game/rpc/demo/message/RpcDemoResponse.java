package cn.itsub.game.rpc.demo.message;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class RpcDemoResponse {
    @Protobuf(fieldType = FieldType.STRING, order = 1)
    private String status;

    @Protobuf(fieldType = FieldType.STRING, order = 2)
    private String detail;

    public RpcDemoResponse() {
    }

    public RpcDemoResponse(String status, String detail) {
        this.status = status;
        this.detail = detail;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    @Override
    public String toString() {
        return "RpcDemoResponse{" +
                "status='" + status + '\'' +
                ", detail='" + detail + '\'' +
                '}';
    }
}
