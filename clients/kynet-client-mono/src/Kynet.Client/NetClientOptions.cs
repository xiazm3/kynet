using System;
using System.Threading;
using System.Threading.Tasks;
using Kynet.Client.Codec;
using Kynet.Client.Protocol;

namespace Kynet.Client
{
    public sealed class NetClientOptions
    {
        public string Endpoint { get; set; } = "tcp://127.0.0.1:30001";
        public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);
        public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(3);
        public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);
        public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(3);
        public bool AutoReconnect { get; set; }
        public TimeSpan ReconnectInitialDelay { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan ReconnectMaxDelay { get; set; } = TimeSpan.FromSeconds(15);
        public int MaxPendingRequests { get; set; } = 1024;
        public int MaxPendingWrites { get; set; } = 256;
        public int MaxFrameLength { get; set; } = 1024 * 1024;
        public int MaxPayloadLength { get; set; } = 512 * 1024;
        public int MaxStringLength { get; set; } = 8 * 1024;
        public IPayloadCodec PayloadCodec { get; set; } = new PayloadCodec();
        public SynchronizationContext? CallbackContext { get; set; }
        public Func<NetClient, CancellationToken, Task>? SessionRecoveryHandler { get; set; }

        internal NetClientOptions Snapshot()
        {
            var copy = (NetClientOptions)MemberwiseClone();
            copy.GetEndpoint();
            ValidateTimeout(copy.ConnectTimeout, nameof(ConnectTimeout));
            ValidateTimeout(copy.RequestTimeout, nameof(RequestTimeout));
            ValidateTimeout(copy.HeartbeatTimeout, nameof(HeartbeatTimeout));
            ValidateTimeout(copy.ReconnectInitialDelay, nameof(ReconnectInitialDelay));
            ValidateTimeout(copy.ReconnectMaxDelay, nameof(ReconnectMaxDelay));
            if (copy.HeartbeatInterval != TimeSpan.Zero) ValidateTimeout(copy.HeartbeatInterval, nameof(HeartbeatInterval));
            if (copy.ReconnectInitialDelay > copy.ReconnectMaxDelay) throw new ArgumentException("Invalid reconnect delay range.");
            if (copy.MaxPendingRequests < 1 || copy.MaxPendingWrites < 1) throw new ArgumentException("Pending limits must be positive.");
            if (copy.PayloadCodec == null) throw new ArgumentNullException(nameof(PayloadCodec));
            copy.CreatePacketCodec();
            return copy;
        }

        internal Uri GetEndpoint()
        {
            if (!Uri.TryCreate(Endpoint, UriKind.Absolute, out Uri uri) || string.IsNullOrEmpty(uri.Host)
                || uri.Port < 1 || !string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Fragment))
                throw new ArgumentException("A gateway endpoint with an explicit port is required.", nameof(Endpoint));
            switch (uri.Scheme)
            {
                case "tcp": case "tcps": case "udp":
                    if (uri.AbsolutePath != "/" || uri.Query.Length != 0) throw new ArgumentException("TCP/UDP endpoints cannot contain a path or query.");
                    break;
                case "ws": case "wss": break;
                default: throw new ArgumentException("Supported gateway schemes: tcp, tcps, udp, ws, wss.");
            }
            return uri;
        }

        internal GatewayPacketCodec CreatePacketCodec() => new GatewayPacketCodec(MaxFrameLength, MaxPayloadLength, MaxStringLength);

        internal static void ValidateTimeout(TimeSpan timeout, string name)
        {
            if (timeout.TotalMilliseconds < 1 || timeout.TotalMilliseconds > int.MaxValue)
                throw new ArgumentOutOfRangeException(name);
        }
    }
}
