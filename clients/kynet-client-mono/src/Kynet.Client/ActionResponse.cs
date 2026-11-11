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

        // 供生成代码在客户端侧完成类型变换后重包一层（如 List 信封解码）。
        public ActionResponse(T data, string? errorCode, string? errorMessage, long sessionId, long userId)
        {
            Data = data;
            ErrorCode = errorCode;
            ErrorMessage = errorMessage;
            SessionId = sessionId;
            UserId = userId;
        }

        public T EnsureSuccess()
        {
            if (!IsSuccess) throw new GatewayRequestException(ErrorCode!, ErrorMessage);
            return Data;
        }

        /// <summary>ioGame 风格别名：等价于 Data，写法兼容弱类型调用习惯。</summary>
        public TResult GetValue<TResult>()
        {
            if (Data is TResult value) return value;
            throw new InvalidCastException("Response data is " + typeof(T).Name + ", not " + typeof(TResult).Name + ".");
        }
    }

    public static class ActionResponseExtensions
    {
        /// <summary>ioGame 风格调试输出；失败时打印错误码，成功时打印数据。返回原响应便于链式调用。</summary>
        public static ActionResponse<T> Log<T>(this ActionResponse<T> response)
        {
            if (response.IsSuccess)
            {
                Console.WriteLine("[Kynet] ok data=" + Describe(response.Data));
            }
            else
            {
                Console.WriteLine("[Kynet] error " + response.ErrorCode + " " + response.ErrorMessage);
            }
            return response;
        }

        private static string Describe<T>(T data)
        {
            if (data == null) return "<null>";
            if (data is byte[] bytes) return "bytes(" + bytes.Length + ")";
            return data.ToString() ?? data.GetType().Name;
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
