using System;
using System.IO;
using System.Text;

namespace Kynet.Client.Protocol
{
    public sealed class GatewayPacketCodec
    {
        public const int Magic = 0x4B594E54;
        public const short Version = 1;
        public const int MinimumPacketLength = 60;
        private static readonly Encoding Utf8 = new UTF8Encoding(false, true);
        public int MaxFrameLength { get; }
        public int MaxPayloadLength { get; }
        public int MaxStringLength { get; }

        // MaxFrameLength includes the TCP four-byte length prefix, as in Netty.
        public GatewayPacketCodec(int maxFrameLength = 1024 * 1024,
            int maxPayloadLength = 512 * 1024, int maxStringLength = 8 * 1024)
        {
            if (maxFrameLength < MinimumPacketLength + 4) throw new ArgumentOutOfRangeException(nameof(maxFrameLength));
            if (maxPayloadLength < 0) throw new ArgumentOutOfRangeException(nameof(maxPayloadLength));
            if (maxStringLength < 0) throw new ArgumentOutOfRangeException(nameof(maxStringLength));
            MaxFrameLength = maxFrameLength;
            MaxPayloadLength = maxPayloadLength;
            MaxStringLength = maxStringLength;
        }

        public byte[] Encode(GatewayPacket packet)
        {
            if (packet == null) throw new ArgumentNullException(nameof(packet));
            if ((byte)packet.Type > (byte)GatewayPacketType.Pong) throw new GatewayProtocolException("Unknown packet type.");
            byte[]? code = EncodeString(packet.ErrorCode);
            byte[]? message = EncodeString(packet.ErrorMessage);
            if (packet.Payload != null && packet.Payload.Length > MaxPayloadLength)
                throw new GatewayProtocolException("Payload exceeds the configured limit.");
            long length = (long)MinimumPacketLength + (code?.Length ?? 0) + (message?.Length ?? 0) + (packet.Payload?.Length ?? 0);
            ValidatePacketLength(length);
            using (var stream = new MemoryStream((int)length))
            {
                WriteInt32(stream, Magic);
                stream.WriteByte(0);
                stream.WriteByte((byte)Version);
                stream.WriteByte((byte)packet.Type);
                stream.WriteByte(packet.OneWay ? (byte)1 : (byte)0);
                WriteInt64(stream, packet.RequestId);
                WriteInt32(stream, packet.Cmd);
                WriteInt32(stream, packet.Act);
                WriteInt64(stream, packet.TimeoutMillis);
                WriteInt64(stream, packet.SessionId);
                WriteInt64(stream, packet.UserId);
                WriteBytes(stream, code);
                WriteBytes(stream, message);
                WriteBytes(stream, packet.Payload);
                return stream.ToArray();
            }
        }

        public GatewayPacket Decode(byte[] data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            ValidatePacketLength(data.Length);
            int offset = 0;
            if (ReadInt32(data, ref offset) != Magic) throw new GatewayProtocolException("Invalid gateway magic.");
            if (data[offset++] != 0 || data[offset++] != Version) throw new GatewayProtocolException("Unsupported gateway version.");
            var type = (GatewayPacketType)data[offset++];
            if (type > GatewayPacketType.Pong) throw new GatewayProtocolException("Unknown gateway packet type.");
            var packet = new GatewayPacket
            {
                Type = type,
                OneWay = data[offset++] != 0,
                RequestId = ReadInt64(data, ref offset),
                Cmd = ReadInt32(data, ref offset),
                Act = ReadInt32(data, ref offset),
                TimeoutMillis = ReadInt64(data, ref offset),
                SessionId = ReadInt64(data, ref offset),
                UserId = ReadInt64(data, ref offset),
                ErrorCode = ReadString(data, ref offset),
                ErrorMessage = ReadString(data, ref offset),
                Payload = ReadBytes(data, ref offset, MaxPayloadLength)
            };
            if (offset != data.Length) throw new GatewayProtocolException("Unexpected trailing packet data.");
            return packet;
        }

        internal void ValidatePacketLength(long length)
        {
            if (length < MinimumPacketLength || length > MaxFrameLength - 4L)
                throw new GatewayProtocolException("Invalid gateway frame length: " + length);
        }

        private byte[]? EncodeString(string? value)
        {
            if (value == null) return null;
            if (Utf8.GetByteCount(value) > MaxStringLength) throw new GatewayProtocolException("String exceeds the configured limit.");
            return Utf8.GetBytes(value);
        }

        private string? ReadString(byte[] data, ref int offset)
        {
            byte[]? value = ReadBytes(data, ref offset, MaxStringLength);
            return value == null ? null : Utf8.GetString(value);
        }

        private static byte[]? ReadBytes(byte[] data, ref int offset, int maximum)
        {
            int length = ReadInt32(data, ref offset);
            if (length == -1) return null;
            if (length < 0 || length > maximum || length > data.Length - offset)
                throw new GatewayProtocolException("Invalid or truncated variable-length field.");
            var value = new byte[length];
            Buffer.BlockCopy(data, offset, value, 0, length);
            offset += length;
            return value;
        }

        internal static int ReadInt32(byte[] data, ref int offset)
        {
            if (data.Length - offset < 4) throw new GatewayProtocolException("Truncated integer.");
            int value = (data[offset] << 24) | (data[offset + 1] << 16) | (data[offset + 2] << 8) | data[offset + 3];
            offset += 4;
            return value;
        }

        private static long ReadInt64(byte[] data, ref int offset)
        {
            uint high = unchecked((uint)ReadInt32(data, ref offset));
            uint low = unchecked((uint)ReadInt32(data, ref offset));
            return unchecked((long)(((ulong)high << 32) | low));
        }

        internal static void WriteInt32(Stream stream, int value)
        {
            stream.WriteByte((byte)(value >> 24));
            stream.WriteByte((byte)(value >> 16));
            stream.WriteByte((byte)(value >> 8));
            stream.WriteByte((byte)value);
        }

        private static void WriteInt64(Stream stream, long value)
        {
            WriteInt32(stream, unchecked((int)(value >> 32)));
            WriteInt32(stream, unchecked((int)value));
        }

        private static void WriteBytes(Stream stream, byte[]? data)
        {
            WriteInt32(stream, data?.Length ?? -1);
            if (data != null) stream.Write(data, 0, data.Length);
        }
    }
}
