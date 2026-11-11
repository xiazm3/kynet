package cn.itsub.kynet.common.codec;

import com.baidu.bjf.remoting.protobuf.Codec;
import com.baidu.bjf.remoting.protobuf.ProtobufProxy;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ListValueCodecTest {

    @ProtobufClass
    public static class Item {
        public String name;
    }

    @Test
    void stringListWrapsAndRoundTrips() throws Exception {
        byte[] bytes = ProtobufPayloadCodec.encode(List.of("hello", "kynet"));
        ListValue value = decode(bytes);
        assertEquals(ListValue.Kind.STRING, value.getKind());
        assertEquals(List.of("hello", "kynet"), value.getStringValues());
    }

    @Test
    void intListWrapsAndRoundTrips() throws Exception {
        byte[] bytes = ProtobufPayloadCodec.encode(List.of(1, 2, 3));
        ListValue value = decode(bytes);
        assertEquals(ListValue.Kind.INT, value.getKind());
        assertEquals(List.of(1, 2, 3), value.getIntValues());
    }

    @Test
    void emptyListUsesStringKind() throws Exception {
        byte[] bytes = ProtobufPayloadCodec.encode(List.of());
        ListValue value = decode(bytes);
        assertEquals(ListValue.Kind.STRING, value.getKind());
    }

    @Test
    void messageListEncodesEachElement() throws Exception {
        Item first = new Item();
        first.name = "a";
        Item second = new Item();
        second.name = "b";
        byte[] bytes = ProtobufPayloadCodec.encode(List.of(first, second));
        ListValue value = decode(bytes);
        assertEquals(ListValue.Kind.MESSAGE, value.getKind());
        assertEquals(Item.class.getName(), value.getElementType());
        assertEquals(2, value.getMessageValues().size());
        assertEquals("b", ProtobufPayloadCodec.decode(value.getMessageValues().get(1), Item.class).name);
    }

    @Test
    void mixedElementTypesAreRejected() {
        assertThrows(IllegalStateException.class, () -> ProtobufPayloadCodec.encode(List.of("text", 1)));
    }

    private ListValue decode(byte[] bytes) throws Exception {
        Codec<ListValue> codec = ProtobufProxy.create(ListValue.class);
        return codec.readFrom(com.google.protobuf.CodedInputStream.newInstance(bytes));
    }
}
