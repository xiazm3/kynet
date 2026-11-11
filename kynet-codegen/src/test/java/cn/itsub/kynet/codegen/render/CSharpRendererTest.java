package cn.itsub.kynet.codegen.render;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CSharpRendererTest {

    @Test
    void proxyHasAwaitAndCallbackStyles() throws IOException {
        Path out = RendererTestBase.renderToTemp(new CSharpRenderer(), "csharp");
        String proxy = RendererTestBase.readAll(out, "FixtureMyActionProxy.g.cs");
        assertTrue(proxy.contains(
                "public static async Task<ActionResponse<FixtureUserMessage>> OfAwaitLoginVerify("
                        + "FixtureLoginVerifyMessage fixtureLoginVerifyMessage, NetClient? client = null)"), () -> proxy);
        assertTrue(proxy.contains(
                "public static async void OfLoginVerify(FixtureLoginVerifyMessage fixtureLoginVerifyMessage, "
                        + "Action<ActionResponse<FixtureUserMessage>> callback, NetClient? client = null)"), () -> proxy);
        assertTrue(proxy.contains("net.RequestAsync<int>(9, 2, @int)"), () -> proxy);
        assertTrue(proxy.contains("net.RequestAsync<KynetListValue>(9, 3, @string)"), () -> proxy);
        assertTrue(proxy.contains("KynetListValueDecoder.ToStringList(envelope.EnsureSuccess())"), () -> proxy);
        assertTrue(proxy.contains("Task<ActionResponse<object?>> OfAwaitFireAndForget"), () -> proxy);
        assertTrue(proxy.contains("net.RequestAsync<object?>(9, 5, fixtureLoginVerifyMessage)"), () -> proxy);
        assertTrue(proxy.contains("RequestAsync<FixtureUserMessage>(9, 6)"), () -> proxy);
    }

    @Test
    void messagesUseProtobufNetAttributes() throws IOException {
        Path out = RendererTestBase.renderToTemp(new CSharpRenderer(), "csharp");
        String messages = RendererTestBase.readAll(out, "KynetMessages.g.cs");
        assertTrue(messages.contains("[ProtoContract]"), () -> messages);
        assertTrue(messages.contains("[ProtoMember(1)] public long UserId { get; set; } = 0;"), () -> messages);
        assertTrue(messages.contains("[ProtoMember(5)] public List<string> Tags { get; set; } = new List<string>();"), () -> messages);
        assertTrue(messages.contains("[ProtoMember(6)] public FixtureLoginVerifyMessage? Inner { get; set; }"), () -> messages);
        assertTrue(messages.contains("public enum FixtureLevel"), () -> messages);
        assertTrue(messages.contains("FixtureLevel Level { get; set; } = (FixtureLevel)0"), () -> messages);
        assertTrue(messages.contains("public sealed class KynetListValue"), () -> messages);
        assertTrue(messages.contains("ToMessageList<T>"), () -> messages);
    }

    @Test
    void runtimeExposesDefaultClient() throws IOException {
        Path out = RendererTestBase.renderToTemp(new CSharpRenderer(), "csharp");
        String runtime = RendererTestBase.readAll(out, "KynetGeneratedRuntime.g.cs");
        assertTrue(runtime.contains("public static NetClient? DefaultClient { get; set; }"), () -> runtime);
    }
}
