package cn.itsub.kynet.common.protocol;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

import java.util.List;

@ProtobufClass
public class GatewayPushMessage {
    @Protobuf(fieldType = FieldType.INT64, order = 1)
    private long sessionId;

    @Protobuf(fieldType = FieldType.INT64, order = 2)
    private long userId;

    @Protobuf(fieldType = FieldType.INT64, order = 3)
    private List<Long> playerIds;

    @Protobuf(fieldType = FieldType.INT32, order = 4)
    private int cmd;

    @Protobuf(fieldType = FieldType.INT32, order = 5)
    private int act;

    @Protobuf(fieldType = FieldType.BYTES, order = 6)
    private byte[] payload;

    @Protobuf(fieldType = FieldType.STRING, order = 7)
    private String payloadType;

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

    public List<Long> getPlayerIds() {
        return playerIds;
    }

    public void setPlayerIds(List<Long> playerIds) {
        this.playerIds = playerIds;
    }

    public int getCmd() {
        return cmd;
    }

    public void setCmd(int cmd) {
        this.cmd = cmd;
    }

    public int getAct() {
        return act;
    }

    public void setAct(int act) {
        this.act = act;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }
}
