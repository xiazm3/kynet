package cn.itsub.kynet.codegen.render;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TypeScriptRendererTest {

    @Test
    void messagesHaveInterfacesAndCodecs() throws IOException {
        Path out = RendererTestBase.renderToTemp(new TypeScriptRenderer(), "ts");
        String messages = RendererTestBase.readAll(out, "kynet-messages.ts");
        assertTrue(messages.contains("export interface FixtureUserMessage {"), () -> messages);
        assertTrue(messages.contains("userId: bigint;"), () -> messages);
        assertTrue(messages.contains("tags: string[];"), () -> messages);
        assertTrue(messages.contains("inner?: FixtureLoginVerifyMessage;"), () -> messages);
        assertTrue(messages.contains("export const fixtureUserMessage = {"), () -> messages);
        assertTrue(messages.contains("export const FixtureLevel = { LOW: 0, HIGH: 1 } as const;"), () -> messages);
        assertTrue(messages.contains("writer.uint32(8).int64(message.userId.toString());"), () -> messages);
    }

    @Test
    void controllerHasCallbackAndPromiseStyles() throws IOException {
        Path out = RendererTestBase.renderToTemp(new TypeScriptRenderer(), "ts");
        String controller = RendererTestBase.readAll(out, "fixture-my-action.ts");
        assertTrue(controller.contains("import { ActionResponse, KynetClient } from \"../client.js\";"), () -> controller);
        assertTrue(controller.contains("async ofAwaitLoginVerify(client: KynetClient, fixtureLoginVerifyMessageArg: FixtureLoginVerifyMessage, "
                + "options?: RequestOptions): Promise<TypedResponse<FixtureUserMessage>>"), () -> controller);
        assertTrue(controller.contains("await client.request(9, 1, fixtureLoginVerifyMessage.encode(fixtureLoginVerifyMessageArg), options);"),
                () -> controller);
        assertTrue(controller.contains("await client.request(9, 2, encodeInt32Payload(int), options);"), () -> controller);
        assertTrue(controller.contains("extractKynetListString(data ?? new Uint8Array(0))"), () -> controller);
        assertTrue(controller.contains("extractKynetListMessages(data ?? new Uint8Array(0), fixtureUserMessage.decode)"),
                () -> controller);
        assertTrue(controller.contains("await client.request(9, 5, fixtureLoginVerifyMessage.encode(fixtureLoginVerifyMessageArg), options);"),
                () -> controller);
        assertTrue(controller.contains("ensureSuccess()"), () -> controller);
    }
}
