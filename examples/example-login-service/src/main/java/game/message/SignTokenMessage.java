package game.message;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class SignTokenMessage {
    @Protobuf(fieldType = FieldType.STRING, order = 1)
    private String ticket;

    public String getTicket() {
        return ticket;
    }

    public void setTicket(String ticket) {
        this.ticket = ticket;
    }

    @Override
    public String toString() {
        return "SignTokenMessage{ticket='" + ticket + "'}";
    }
}
