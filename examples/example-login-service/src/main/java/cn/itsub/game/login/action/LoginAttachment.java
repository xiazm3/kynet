package cn.itsub.game.login.action;

import cn.itsub.kynet.framework.action.BaseAttachment;
import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

@ProtobufClass
public class LoginAttachment extends BaseAttachment {
    @Protobuf(fieldType = FieldType.STRING, order = 2)
    private String username;

    @Protobuf(fieldType = FieldType.STRING, order = 3)
    private String token;

    @Protobuf(fieldType = FieldType.INT64, order = 4)
    private long loginTimeMillis;

    @Protobuf(fieldType = FieldType.INT32, order = 5)
    private int helloCount;

    @Protobuf(fieldType = FieldType.STRING, order = 6)
    private String cResult;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public long getLoginTimeMillis() {
        return loginTimeMillis;
    }

    public void setLoginTimeMillis(long loginTimeMillis) {
        this.loginTimeMillis = loginTimeMillis;
    }

    public int getHelloCount() {
        return helloCount;
    }

    public void setHelloCount(int helloCount) {
        this.helloCount = helloCount;
    }

    public String getCResult() {
        return cResult;
    }

    public void setCResult(String cResult) {
        this.cResult = cResult;
    }
}
