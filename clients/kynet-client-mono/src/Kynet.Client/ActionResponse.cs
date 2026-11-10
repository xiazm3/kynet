using System;
using Kynet.Client.Codec;
using Kynet.Client.Protocol;

namespace Kynet.Client
{
    public enum ClientState { Disconnected, Connecting, Recovering, Connected, Reconnecting, Closed }

    public sealed class ClientSession
    {
        public long SessionId { get; }
        public long UserId { get; }
        public bool IsAuthenticated => UserId > 0;
        internal ClientSession(long sessionId = 0, long userId = 0) { SessionId = sessionId; UserId = userId; }
    }

    public sealed class ActionResponse<T>
    {
        public bool IsSuccess => ErrorCode == null;
        public T Data { get; }
        public string? ErrorCode { get; }
        public string? ErrorMessage { get; }
        public long SessionId { get; }
        public long UserId { get; }

        internal ActionResponse(GatewayPacket packet, IPayloadCodec codec)
        {
            ErrorCode = packet.ErrorCode;
            ErrorMessage = packet.ErrorMessage;
            SessionId = packet.SessionId;
            UserId = packet.UserId;
            Data = IsSuccess ? codec.Decode<T>(packet.Payload) : default!;
        }

        public T EnsureSuccess()
        {
            if (!IsSuccess) throw new GatewayRequestException(ErrorCode!, ErrorMessage);
            return Data;
        }
    }

    public sealed class GatewayRequestException : Exception
    {
        public string ErrorCode { get; }
        public GatewayRequestException(string code, string? message) : base(code + ": " + message) { ErrorCode = code; }
    }

    public sealed class GatewayDisconnectedException : System.IO.IOException
    {
        public GatewayDisconnectedException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
