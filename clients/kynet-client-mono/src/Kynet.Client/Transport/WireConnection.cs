using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using Kynet.Client.Protocol;

namespace Kynet.Client.Transport
{
    internal sealed class WireConnection : IDisposable
    {
        private readonly Uri _endpoint;
        private readonly GatewayPacketCodec _codec;
        private TcpClient? _tcp;
        private Stream? _stream;
        private UdpClient? _udp;
        private ClientWebSocket? _webSocket;

        public WireConnection(Uri endpoint, GatewayPacketCodec codec) { _endpoint = endpoint; _codec = codec; }

        public async Task ConnectAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string host = _endpoint.DnsSafeHost;
            switch (_endpoint.Scheme)
            {
                case "ws": case "wss":
                    _webSocket = new ClientWebSocket();
                    using (cancellationToken.Register(Dispose))
                        await _webSocket.ConnectAsync(_endpoint, cancellationToken).ConfigureAwait(false);
                    break;
                case "udp":
                    // Resolve before creating the socket so IPv4 and IPv6 both work on Mono.
                    var addresses = await System.Net.Dns.GetHostAddressesAsync(host).ConfigureAwait(false);
                    cancellationToken.ThrowIfCancellationRequested();
                    _udp = new UdpClient(addresses[0].AddressFamily);
                    _udp.Connect(addresses[0], _endpoint.Port);
                    break;
                default:
                    _tcp = new TcpClient { NoDelay = true };
                    using (cancellationToken.Register(Dispose))
                    {
                        await _tcp.ConnectAsync(host, _endpoint.Port).ConfigureAwait(false);
                        cancellationToken.ThrowIfCancellationRequested();
                        _stream = _tcp.GetStream();
                        if (_endpoint.Scheme == "tcps")
                        {
                            // Use OS trust and hostname validation; never accept arbitrary certificates.
                            var tls = new SslStream(_stream, false);
                            _stream = tls;
                            await tls.AuthenticateAsClientAsync(host, null, SslProtocols.None, true).ConfigureAwait(false);
                        }
                    }
                    break;
            }
            cancellationToken.ThrowIfCancellationRequested();
        }

        // The client serializes writes. Cancelling an active write closes the connection,
        // since a partially transmitted stream frame cannot be reused safely.
        public async Task SendAsync(byte[] packet, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_udp != null && packet.Length > 65507) throw new GatewayProtocolException("UDP packet exceeds 65507 bytes.");
            using (cancellationToken.Register(Dispose))
            {
                if (_webSocket != null)
                    await _webSocket.SendAsync(new ArraySegment<byte>(packet), WebSocketMessageType.Binary, true, cancellationToken).ConfigureAwait(false);
                else if (_udp != null)
                    await _udp.SendAsync(packet, packet.Length).ConfigureAwait(false);
                else
                {
                    var frame = new byte[packet.Length + 4];
                    using (var stream = new MemoryStream(frame, true)) GatewayPacketCodec.WriteInt32(stream, packet.Length);
                    Buffer.BlockCopy(packet, 0, frame, 4, packet.Length);
                    await _stream!.WriteAsync(frame, 0, frame.Length, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task<GatewayPacket> ReceiveAsync(CancellationToken cancellationToken)
        {
            byte[] packet;
            if (_webSocket != null)
            {
                var chunk = new byte[Math.Min(8192, _codec.MaxFrameLength)];
                using (var stream = new MemoryStream())
                {
                    WebSocketReceiveResult result;
                    do
                    {
                        result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(chunk), cancellationToken).ConfigureAwait(false);
                        if (result.MessageType == WebSocketMessageType.Close) throw new EndOfStreamException("Gateway closed the WebSocket.");
                        if (result.MessageType != WebSocketMessageType.Binary) throw new GatewayProtocolException("Expected a binary WebSocket message.");
                        if (stream.Length + result.Count > _codec.MaxFrameLength - 4L)
                            throw new GatewayProtocolException("WebSocket message exceeds the frame limit.");
                        stream.Write(chunk, 0, result.Count);
                    } while (!result.EndOfMessage);
                    packet = stream.ToArray();
                }
            }
            else if (_udp != null)
            {
                using (cancellationToken.Register(Dispose))
                    packet = (await _udp.ReceiveAsync().ConfigureAwait(false)).Buffer;
            }
            else
            {
                var prefix = new byte[4];
                await ReadExactlyAsync(prefix, cancellationToken).ConfigureAwait(false);
                int offset = 0;
                int length = GatewayPacketCodec.ReadInt32(prefix, ref offset);
                _codec.ValidatePacketLength(length);
                packet = new byte[length];
                await ReadExactlyAsync(packet, cancellationToken).ConfigureAwait(false);
            }
            return _codec.Decode(packet);
        }

        private async Task ReadExactlyAsync(byte[] buffer, CancellationToken cancellationToken)
        {
            int offset = 0;
            while (offset < buffer.Length)
            {
                int count = await _stream!.ReadAsync(buffer, offset, buffer.Length - offset, cancellationToken).ConfigureAwait(false);
                if (count == 0) throw new EndOfStreamException("Gateway closed the TCP connection.");
                offset += count;
            }
        }

        public void Dispose()
        {
            // Cancellation callbacks can race with the receive loop and explicit disconnect.
            try { _webSocket?.Abort(); } catch (ObjectDisposedException) { }
            _webSocket?.Dispose();
            _udp?.Dispose();
            _tcp?.Dispose();
            _stream?.Dispose();
        }
    }
}
