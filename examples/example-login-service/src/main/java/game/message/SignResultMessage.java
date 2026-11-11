package game.message;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class SignResultMessage {
    @Protobuf(fieldType = FieldType.INT32, order = 1)
    private int day;

    @Protobuf(fieldType = FieldType.STRING, order = 2)
    private String bonus;

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public String getBonus() {
        return bonus;
    }

    public void setBonus(String bonus) {
        this.bonus = bonus;
    }

    @Override
    public String toString() {
        return "SignResultMessage{day=" + day + ", bonus='" + bonus + "'}";
    }
}
