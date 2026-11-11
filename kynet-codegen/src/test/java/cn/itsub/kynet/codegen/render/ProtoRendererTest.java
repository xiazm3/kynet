package cn.itsub.kynet.codegen.render;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtoRendererTest {

    @Test
    void messagesUseJprotobufOrderAsFieldNumbers() throws IOException {
        Path out = RendererTestBase.renderToTemp(new ProtoRenderer(), "proto");
        String proto = RendererTestBase.readAll(out, ProtoRenderer.OUTPUT);
        assertTrue(proto.contains("message FixtureUserMessage {"), () -> proto);
        assertTrue(proto.contains("int64 user_id = 1;") || proto.contains("int64 userId = 1;"), () -> proto);
        assertTrue(proto.contains("repeated string tags = 5;"), () -> proto);
        assertTrue(proto.contains("FixtureLoginVerifyMessage inner = 6;"), () -> proto);
        assertTrue(proto.contains("enum FixtureLevel {"), () -> proto);
        assertTrue(proto.contains("LOW = 0;"), () -> proto);
    }

    @Test
    void actionsMapToServicesWithEnvelopeNotes() throws IOException {
        Path out = RendererTestBase.renderToTemp(new ProtoRenderer(), "proto");
        String proto = RendererTestBase.readAll(out, ProtoRenderer.OUTPUT);
        assertTrue(proto.contains("service FixtureMyAction {"), () -> proto);
        assertTrue(proto.contains("rpc LoginVerify(FixtureLoginVerifyMessage) returns (FixtureUserMessage);"), () -> proto);
        assertTrue(proto.contains("rpc IntValue(KynetPrimitiveValue) returns (KynetPrimitiveValue);"), () -> proto);
        assertTrue(proto.contains("rpc ListString(KynetRawString) returns (KynetListValue);"), () -> proto);
        assertTrue(proto.contains("rpc ListUsers(KynetRawString) returns (KynetListValue);"), () -> proto);
        assertTrue(proto.contains("rpc FireAndForget(FixtureLoginVerifyMessage) returns (google.protobuf.Empty);"), () -> proto);
        assertTrue(proto.contains("rpc ContextOnly(google.protobuf.Empty) returns (FixtureUserMessage);"), () -> proto);
        assertTrue(proto.contains("cmd=9 act=1"), () -> proto);
        assertTrue(proto.contains("import \"google/protobuf/empty.proto\";"), () -> proto);
    }
}
