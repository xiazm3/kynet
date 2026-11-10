using System;
using System.IO;

namespace Kynet.Client.Protocol
{
    public enum GatewayPacketType : byte { Request, Response, Push, Ping, Pong }

    public sealed class GatewayPacket
    {
        public GatewayPacketType Type { get; set; }
        public bool OneWay { get; set; }
        public long RequestId { get; set; }
        public int Cmd { get; set; }
        public int Act { get; set; }
        public long TimeoutMillis { get; set; }
        public long SessionId { get; set; }
        public long UserId { get; set; }
        public string? ErrorCode { get; set; }
        public string? ErrorMessage { get; set; }
        public byte[]? Payload { get; set; }
    }

    public static class ActionKey
    {
        public static int Of(int cmd, int act)
        {
            if ((uint)cmd > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(cmd));
            if ((uint)act > ushort.MaxValue) throw new ArgumentOutOfRangeException(nameof(act));
            return (cmd << 16) | act;
        }

        public static int CmdOf(int messageId) => (int)((uint)messageId >> 16);
        public static int ActOf(int messageId) => messageId & ushort.MaxValue;
    }

    public sealed class GatewayProtocolException : IOException
    {
        public GatewayProtocolException(string message) : base(message) { }
    }
}
