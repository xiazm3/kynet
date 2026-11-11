package cn.itsub.kynet.codegen.render;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

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
    }

    @Test
    void controllerHasAwaitAndCallbackStyles() throws IOException {
        Path out = RendererTestBase.renderToTemp(new GdScriptRenderer(), "gd");
        String controller = RendererTestBase.readAll(out, "fixture_my_action.gd");
        assertTrue(controller.contains("class_name FixtureMyAction"), () -> controller);
        assertTrue(controller.contains("static func of_await_login_verify(client, fixture_login_verify_message: Dictionary) -> Dictionary:"),
                () -> controller);
        assertTrue(controller.contains("await client.request_async(9, 1, Messages.encode_fixture_login_verify_message(fixture_login_verify_message))"),
                () -> controller);
        assertTrue(controller.contains("await client.request_async(9, 2, Messages.encode_int_payload(param_int))"), () -> controller);
        assertTrue(controller.contains("Messages.unwrap(result, \"decode_fixture_user_message\")"), () -> controller);
        assertTrue(controller.contains("Messages.unwrap(result, \"decode_list_string\")"), () -> controller);
        assertTrue(controller.contains("callback.call(result)"), () -> controller);
    }
}
