package cn.itsub.kynet.codegen.fixture;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;

import java.util.List;

/** 扫描器/渲染器测试的消息夹具。 */
public final class FixtureMessages {
    private FixtureMessages() {
    }

    public enum FixtureLevel {
        LOW,
        HIGH
    }

    @ProtobufClass
    public static class FixtureLoginVerifyMessage {
        @Protobuf(fieldType = FieldType.STRING, order = 1)
        private String jwt;

        public String getJwt() {
            return jwt;
        }

        public void setJwt(String jwt) {
            this.jwt = jwt;
        }
    }

    @ProtobufClass
    public static class FixtureUserMessage {
        @Protobuf(fieldType = FieldType.INT64, order = 1)
        private long userId;

        @Protobuf(fieldType = FieldType.STRING, order = 2)
        private String username;

        @Protobuf(fieldType = FieldType.STRING, order = 3)
        private String token;

        @Protobuf(fieldType = FieldType.ENUM, order = 4)
        private FixtureLevel level;

        @Protobuf(fieldType = FieldType.STRING, order = 5)
        private List<String> tags;

        @Protobuf(fieldType = FieldType.OBJECT, order = 6)
        private FixtureLoginVerifyMessage inner;

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

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

        public FixtureLevel getLevel() {
            return level;
        }

        public void setLevel(FixtureLevel level) {
            this.level = level;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public FixtureLoginVerifyMessage getInner() {
            return inner;
        }

        public void setInner(FixtureLoginVerifyMessage inner) {
            this.inner = inner;
        }
    }

    /** 混合显式/隐式 order：隐式从 max(显式 order=4)+1 起按声明顺序递增（jprotobuf 实测规则）。 */
    @ProtobufClass
    public static class FixtureMixed {
        @Protobuf(fieldType = FieldType.STRING)
        private String implicitA;

        @Protobuf(fieldType = FieldType.STRING, order = 3)
        private String explicitC;

        @Protobuf(fieldType = FieldType.STRING)
        private String implicitB;

        @Protobuf(fieldType = FieldType.INT32, order = 2)
        private Integer score;

        @Protobuf(fieldType = FieldType.STRING, order = 4)
        private List<String> tags;

        public String getImplicitA() {
            return implicitA;
        }

        public void setImplicitA(String implicitA) {
            this.implicitA = implicitA;
        }

        public String getExplicitC() {
            return explicitC;
        }

        public void setExplicitC(String explicitC) {
            this.explicitC = explicitC;
        }

        public String getImplicitB() {
            return implicitB;
        }

        public void setImplicitB(String implicitB) {
            this.implicitB = implicitB;
        }

        public Integer getScore() {
            return score;
        }

        public void setScore(Integer score) {
            this.score = score;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }
    }
}
