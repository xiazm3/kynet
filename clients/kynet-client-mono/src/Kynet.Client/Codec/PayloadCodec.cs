using System;
using System.IO;
using System.Text;
using Google.Protobuf;
using ProtoBuf;

namespace Kynet.Client.Codec
{
    public interface IPayloadCodec
    {
        byte[]? Encode(object? value);
        T Decode<T>(byte[]? data);
    }

    public sealed class PayloadCodec : IPayloadCodec
    {
        private static readonly Encoding Utf8 = new UTF8Encoding(false, true);

        public byte[]? Encode(object? value)
        {
            if (value == null) return null;
            if (value is byte[] bytes) return bytes;
            if (value is string text) return Utf8.GetBytes(text);
            if (value is IMessage message) return message.ToByteArray();
            object encoded = PrimitiveValue.Wrap(value) ?? value;
            if (!Attribute.IsDefined(encoded.GetType(), typeof(ProtoContractAttribute)))
                throw new NotSupportedException("Use IMessage, [ProtoContract], raw bytes, or a custom IPayloadCodec: " + value.GetType());
            using (var stream = new MemoryStream())
            {
                Serializer.NonGeneric.Serialize(stream, encoded);
                return stream.ToArray();
            }
        }

        public T Decode<T>(byte[]? data)
        {
            if (data == null) return default!;
            Type type = typeof(T);
            if (type == typeof(byte[])) return (T)(object)data;
            if (type == typeof(string)) return (T)(object)Utf8.GetString(data);
            if (typeof(IMessage).IsAssignableFrom(type))
            {
                var message = (IMessage)Activator.CreateInstance(type)!;
                message.MergeFrom(data);
                return (T)message;
            }
            using (var stream = new MemoryStream(data, false))
            {
                if (PrimitiveValue.IsPrimitive(type))
                    return (T)Serializer.Deserialize<PrimitiveValue>(stream).Unwrap(type);
                if (!Attribute.IsDefined(type, typeof(ProtoContractAttribute)))
                    throw new NotSupportedException("Response type must implement IMessage or declare [ProtoContract]: " + type);
                return (T)Serializer.NonGeneric.Deserialize(type, stream);
            }
        }
    }

    // Matches cn.itsub.kynet.common.codec.PrimitiveValue, not Google wrapper messages.
    [ProtoContract]
    internal sealed class PrimitiveValue
    {
        [ProtoMember(1, IsRequired = true)] public int Kind { get; set; }
        [ProtoMember(2)] public int? IntValue { get; set; }
        [ProtoMember(3)] public long? LongValue { get; set; }
        [ProtoMember(4)] public bool? BoolValue { get; set; }
        [ProtoMember(5)] public double? DoubleValue { get; set; }
        [ProtoMember(6)] public float? FloatValue { get; set; }

        public static bool IsPrimitive(Type type) => type == typeof(int) || type == typeof(long)
            || type == typeof(bool) || type == typeof(double) || type == typeof(float);

        public static PrimitiveValue? Wrap(object value)
        {
            if (value is int i) return new PrimitiveValue { Kind = 0, IntValue = i };
            if (value is long l) return new PrimitiveValue { Kind = 1, LongValue = l };
            if (value is bool b) return new PrimitiveValue { Kind = 2, BoolValue = b };
            if (value is double d) return new PrimitiveValue { Kind = 3, DoubleValue = d };
            if (value is float f) return new PrimitiveValue { Kind = 4, FloatValue = f };
            return null;
        }

        public object Unwrap(Type type)
        {
            if (type == typeof(int) && Kind == 0) return IntValue ?? 0;
            if (type == typeof(long) && Kind == 1) return LongValue ?? 0L;
            if (type == typeof(bool) && Kind == 2) return BoolValue ?? false;
            if (type == typeof(double) && Kind == 3) return DoubleValue ?? 0d;
            if (type == typeof(float) && Kind == 4) return FloatValue ?? 0f;
            throw new InvalidDataException("PrimitiveValue kind does not match " + type);
        }
    }
}
