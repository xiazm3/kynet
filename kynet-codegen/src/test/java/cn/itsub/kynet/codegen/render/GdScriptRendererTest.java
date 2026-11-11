package cn.itsub.kynet.codegen.render;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GdScriptRendererTest {

    @Test
    void messagesUseProtobufPrimitives() throws IOException {
        Path out = RendererTestBase.renderToTemp(new GdScriptRenderer(), "gd");
        String messages = RendererTestBase.readAll(out, "kynet_messages.gd");
        assertTrue(messages.contains("static func encode_fixture_user_message(message: Dictionary) -> PackedByteArray:"),
                () -> messages);
        assertTrue(messages.contains("Protobuf.varint_field(1, int(message.get(\"userId\", 0)))"), () -> messages);
        assertTrue(messages.contains("Protobuf.string_field(2, message.get(\"username\", \"\"))"), () -> messages);
        assertTrue(messages.contains("decode_fixture_user_message"), () -> messages);
        assertTrue(messages.contains("static func decode_list_string(data: PackedByteArray) -> Dictionary:"), () -> messages);
        assertTrue(messages.contains("static func decode_list_fixture_user_message(data: PackedByteArray) -> Dictionary:"),
                () -> messages);
        assertTrue(messages.contains("static func unwrap(result: Dictionary, decoder: Callable) -> Dictionary:"), () -> messages);
        assertTrue(messages.contains("var decoded: Dictionary = decoder.call(payload)"), () -> messages);
    }

    @Test
    void controllerUsesAwaitOnly() throws IOException {
        Path out = RendererTestBase.renderToTemp(new GdScriptRenderer(), "gd");
        String controller = RendererTestBase.readAll(out, "fixture_my_action.gd");
        assertTrue(controller.contains("class_name FixtureMyAction"), () -> controller);
        assertTrue(controller.contains("static func login_verify(client, fixture_login_verify_message: Dictionary) -> Dictionary:"),
                () -> controller);
        assertTrue(controller.contains("static func context_only(client) -> Dictionary:"), () -> controller);
        assertTrue(controller.contains("await client.request_async(9, 6, null)"), () -> controller);
        assertTrue(controller.contains("static func sign(client, param_int: int) -> Dictionary:"), () -> controller);
        assertFalse(controller.contains("of_"), () -> controller);
        assertFalse(controller.contains("_with_callback"), () -> controller);
        assertFalse(controller.contains("callback"), () -> controller);
        assertTrue(controller.contains("await client.request_async(9, 1, Messages.encode_fixture_login_verify_message(fixture_login_verify_message))"),
                () -> controller);
        assertTrue(controller.contains("await client.request_async(9, 2, Messages.encode_int_payload(param_int))"), () -> controller);
        assertTrue(controller.contains("Messages.unwrap(result, Messages.decode_fixture_user_message)"), () -> controller);
        assertTrue(controller.contains("Messages.unwrap(result, Messages.decode_list_string)"), () -> controller);
    }
}
