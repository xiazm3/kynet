using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Kynet.Client.Protocol;
using Xunit;

namespace Kynet.Client.Tests;

public class NetworkTests
{
    [Fact]
    public async Task TcpHandlesSplitFramesConcurrentRequestsPushAndBusinessError()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);
        server.Start();
        using var client = NewClient(server);
        var pushed = NewCompletion();
        var callbackErrors = NewCompletion();
        client.Error += _ => callbackErrors.TrySetResult(true);
        using var broken = client.Subscribe(5, 6, _ => throw new InvalidOperationException("handler"));
        using var subscription = client.Subscribe<string>(5, 6, text => pushed.TrySetResult(text == "push"));
        Task serving = Task.Run(async () =>
        {
            using var peer = await server.AcceptTcpClientAsync();
            var stream = peer.GetStream();
            await Handshake(stream);
            var requests = new List<GatewayPacket>();
            for (int i = 0; i < 8; i++) requests.Add(await ReadPacket(stream));
            await WritePacket(stream, new GatewayPacket { Type = GatewayPacketType.Push, Cmd = 5, Act = 6,
                SessionId = 0, UserId = 0, Payload = System.Text.Encoding.UTF8.GetBytes("push") });
            foreach (var request in requests.AsEnumerable().Reverse())
                await WritePacket(stream, Reply(request, request.Payload));
            var denied = await ReadPacket(stream);
            var error = Reply(denied);
            error.ErrorCode = "UNAUTHORIZED";
            error.ErrorMessage = "login required";
            await WritePacket(stream, error);
            await stream.ReadAsync(new byte[1]);
        });
        await client.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(123, client.Session.SessionId);
        var responses = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => client.RequestAsync<string>(1, 2, "request-" + i)));
        Assert.Equal(Enumerable.Range(0, 8).Select(i => "request-" + i), responses.Select(r => r.EnsureSuccess()));
        Assert.True(await pushed.Task.WaitAsync(TimeSpan.FromSeconds(3)));
        Assert.True(await callbackErrors.Task.WaitAsync(TimeSpan.FromSeconds(3)));
        var deniedResponse = await client.RequestAsync<string>(1, 2);
        Assert.False(deniedResponse.IsSuccess);
        Assert.Equal("UNAUTHORIZED", Assert.Throws<GatewayRequestException>(() => deniedResponse.EnsureSuccess()).ErrorCode);
        Assert.Equal(0, client.PendingRequestCount);
        await client.DisconnectAsync();
        Assert.Equal(0, client.Session.UserId);
        await serving.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task TimeoutCancellationAndPendingLimitReleaseResources()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);
        server.Start();
        using var client = NewClient(server, options => options.MaxPendingRequests = 1);
        var received = NewCompletion();
        Task serving = Task.Run(async () =>
        {
            using var peer = await server.AcceptTcpClientAsync();
            await Handshake(peer.GetStream());
            await ReadPacket(peer.GetStream());
            received.TrySetResult(true);
            try { while (await peer.GetStream().ReadAsync(new byte[1024]) > 0) { } } catch (IOException) { }
        });
        await client.ConnectAsync();
        Task timeout = client.RequestAsync<string>(1, 2, timeout: TimeSpan.FromMilliseconds(100));
        await received.Task.WaitAsync(TimeSpan.FromSeconds(3));
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.RequestAsync<string>(1, 3));
        await Assert.ThrowsAsync<TimeoutException>(() => timeout);
        Assert.Equal(0, client.PendingRequestCount);
        using var cancellation = new CancellationTokenSource();
        Task canceled = client.RequestAsync<string>(1, 2, cancellationToken: cancellation.Token);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled);
        Assert.Equal(0, client.PendingRequestCount);
        await client.DisconnectAsync();
        await serving.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisconnectOrMalformedReplyFailsOutstandingRequest(bool malformed)
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);
        server.Start();
        using var client = NewClient(server);
        Task serving = Task.Run(async () =>
        {
            using var peer = await server.AcceptTcpClientAsync();
            await Handshake(peer.GetStream());
            var request = await ReadPacket(peer.GetStream());
            if (malformed)
            {
                var response = Reply(request);
                response.Act++;
                await WritePacket(peer.GetStream(), response);
            }
        });
        await client.ConnectAsync();
        await Assert.ThrowsAsync<GatewayDisconnectedException>(() => client.RequestAsync<string>(1, 2));
        await client.DisconnectAsync();
        Assert.Equal(0, client.PendingRequestCount);
        Assert.Equal(ClientState.Disconnected, client.State);
        await serving;
    }

    [Fact]
    public async Task ReconnectUsesNewSessionAndRunsRecoveryBeforeReady()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);
        server.Start();
        var recovered = NewCompletion();
        var states = new List<ClientState>();
        using var client = NewClient(server, options =>
        {
            options.AutoReconnect = true;
            options.ReconnectInitialDelay = TimeSpan.FromMilliseconds(10);
            options.SessionRecoveryHandler = async (gateway, token) =>
            {
                Assert.Equal(ClientState.Recovering, gateway.State);
                Assert.Equal(456, gateway.Session.SessionId);
                var response = await gateway.RequestAsync<string>(1, 1, "login", cancellationToken: token);
                Assert.Equal("recovered", response.EnsureSuccess());
            };
        });
        client.StateChanged += state =>
        {
            lock (states)
            {
                states.Add(state);
                if (state == ClientState.Connected && states.Count(s => s == ClientState.Connected) == 2)
                    recovered.TrySetResult(true);
            }
        };
        var closeFirst = NewCompletion();
        Task serving = Task.Run(async () =>
        {
            using (var first = await server.AcceptTcpClientAsync())
            {
                await Handshake(first.GetStream());
                await closeFirst.Task;
            }
            using var second = await server.AcceptTcpClientAsync();
            await Handshake(second.GetStream(), 456);
            var login = await ReadPacket(second.GetStream());
            Assert.Equal(456, login.SessionId);
            Assert.Equal(0, login.UserId);
            var response = Reply(login, System.Text.Encoding.UTF8.GetBytes("recovered"));
            response.SessionId = 456;
            await WritePacket(second.GetStream(), response);
            await second.GetStream().ReadAsync(new byte[1]);
        });
        await client.ConnectAsync();
        closeFirst.TrySetResult(true);
        await recovered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(10001, client.Session.UserId);
        await client.DisconnectAsync();
        await serving.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task ConnectWaitSurvivesFailedReconnectAttempt()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);
        server.Start();
        var beginReconnect = NewCompletion();
        var firstReconnect = NewCompletion();
        var releaseFailedAttempt = NewCompletion();
        using var client = NewClient(server, options =>
        {
            options.AutoReconnect = true;
            options.ReconnectInitialDelay = TimeSpan.FromMilliseconds(20);
        });
        client.StateChanged += state => { if (state == ClientState.Reconnecting) firstReconnect.TrySetResult(true); };
        Task serving = Task.Run(async () =>
        {
            using (var first = await server.AcceptTcpClientAsync())
            {
                await Handshake(first.GetStream());
                await beginReconnect.Task;
            }
            using (var failed = await server.AcceptTcpClientAsync())
            {
                await ReadPacket(failed.GetStream());
                await releaseFailedAttempt.Task;
            }
            using var success = await server.AcceptTcpClientAsync();
            await Handshake(success.GetStream(), 789);
            await success.GetStream().ReadAsync(new byte[1]);
        });
        await client.ConnectAsync();
        beginReconnect.TrySetResult(true);
        await firstReconnect.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Task waiting = client.ConnectAsync();
        releaseFailedAttempt.TrySetResult(true);
        await waiting.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(789, client.Session.SessionId);
        await client.DisconnectAsync();
        await serving.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task UdpSessionReplacementDisconnectsInsteadOfRetainingAuthentication()
    {
        using var server = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        int port = ((IPEndPoint)server.Client.LocalEndPoint!).Port;
        using var client = new NetClient(new NetClientOptions
        {
            Endpoint = $"udp://127.0.0.1:{port}", HeartbeatInterval = TimeSpan.FromMilliseconds(20)
        });
        var disconnected = NewCompletion();
        client.StateChanged += state => { if (state == ClientState.Disconnected) disconnected.TrySetResult(true); };
        Task serving = Task.Run(async () =>
        {
            var codec = new GatewayPacketCodec();
            var first = await server.ReceiveAsync();
            await server.SendAsync(codec.Encode(Pong(codec.Decode(first.Buffer), 123)), first.RemoteEndPoint);
            var heartbeat = await server.ReceiveAsync();
            await server.SendAsync(codec.Encode(Pong(codec.Decode(heartbeat.Buffer), 456)), heartbeat.RemoteEndPoint);
        });
        await client.ConnectAsync();
        await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(0, client.Session.SessionId);
        await client.DisconnectAsync();
        await serving;
    }

    [Fact]
    public async Task HeartbeatTimeoutDisconnectsSilentPeer()
    {
        using var server = new TcpListener(IPAddress.Loopback, 0);
        server.Start();
        using var client = NewClient(server, options =>
        {
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatTimeout = TimeSpan.FromMilliseconds(80);
        });
        var disconnected = NewCompletion();
        client.StateChanged += state => { if (state == ClientState.Disconnected) disconnected.TrySetResult(true); };
        Task serving = Task.Run(async () =>
        {
            using var peer = await server.AcceptTcpClientAsync();
            await Handshake(peer.GetStream());
            Assert.Equal(GatewayPacketType.Ping, (await ReadPacket(peer.GetStream())).Type);
            await peer.GetStream().ReadAsync(new byte[1]);
        });
        await client.ConnectAsync();
        await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.Equal(0, client.Session.SessionId);
        await client.DisconnectAsync();
        await serving.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task UdpUsesUnprefixedDatagramsAndOneWayHasNoPendingRequest()
    {
        using var server = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        int port = ((IPEndPoint)server.Client.LocalEndPoint!).Port;
        using var client = new NetClient(new NetClientOptions { Endpoint = $"udp://127.0.0.1:{port}", HeartbeatInterval = TimeSpan.Zero });
        Task serving = Task.Run(async () =>
        {
            var codec = new GatewayPacketCodec();
            var ping = await server.ReceiveAsync();
            Assert.Equal(GatewayPacketType.Ping, codec.Decode(ping.Buffer).Type);
            byte[] pong = codec.Encode(Pong(codec.Decode(ping.Buffer), 987));
            await server.SendAsync(pong, ping.RemoteEndPoint);
            var data = await server.ReceiveAsync();
            var request = codec.Decode(data.Buffer);
            Assert.True(request.OneWay);
            Assert.Equal(987, request.SessionId);
        });
        await client.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(3));
        await client.SendOneWayAsync(1, 2, "update");
        Assert.Equal(0, client.PendingRequestCount);
        await serving.WaitAsync(TimeSpan.FromSeconds(3));
        await client.DisconnectAsync();
    }

    [Fact]
    public async Task WebSocketReassemblesBinaryFragments()
    {
        using var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        int port = ((IPEndPoint)reservation.LocalEndpoint).Port;
        reservation.Stop();
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://127.0.0.1:{port}/ws/");
        listener.Start();
        using var client = new NetClient(new NetClientOptions { Endpoint = $"ws://127.0.0.1:{port}/ws/", HeartbeatInterval = TimeSpan.Zero });
        Task serving = Task.Run(async () =>
        {
            var context = await listener.GetContextAsync();
            using var socket = (await context.AcceptWebSocketAsync(null)).WebSocket;
            var buffer = new byte[4096];
            var codec = new GatewayPacketCodec();
            var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            var ping = codec.Decode(buffer.Take(result.Count).ToArray());
            byte[] pong = codec.Encode(Pong(ping, 321));
            await socket.SendAsync(new ArraySegment<byte>(pong, 0, 17), WebSocketMessageType.Binary, false, CancellationToken.None);
            await socket.SendAsync(new ArraySegment<byte>(pong, 17, pong.Length - 17), WebSocketMessageType.Binary, true, CancellationToken.None);
            try { await socket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None); } catch (WebSocketException) { }
        });
        await client.ConnectAsync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(321, client.Session.SessionId);
        await client.DisconnectAsync();
        await serving.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task TlsRejectsUntrustedCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        using var server = new TcpListener(IPAddress.Loopback, 0);
        server.Start();
        int port = ((IPEndPoint)server.LocalEndpoint).Port;
        using var client = new NetClient(new NetClientOptions { Endpoint = $"tcps://127.0.0.1:{port}" });
        Task serving = Task.Run(async () =>
        {
            using var peer = await server.AcceptTcpClientAsync();
            using var tls = new SslStream(peer.GetStream());
            try { await tls.AuthenticateAsServerAsync(certificate); } catch (Exception) { }
        });
        await Assert.ThrowsAnyAsync<Exception>(() => client.ConnectAsync());
        await client.DisconnectAsync();
        await serving.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static NetClient NewClient(TcpListener server, Action<NetClientOptions>? configure = null)
    {
        var options = new NetClientOptions
        {
            Endpoint = $"tcp://127.0.0.1:{((IPEndPoint)server.LocalEndpoint).Port}",
            HeartbeatInterval = TimeSpan.Zero
        };
        configure?.Invoke(options);
        return new NetClient(options);
    }

    private static TaskCompletionSource<bool> NewCompletion() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task Handshake(Stream stream, long sessionId = 123)
    {
        var ping = await ReadPacket(stream);
        Assert.Equal(GatewayPacketType.Ping, ping.Type);
        await WritePacket(stream, Pong(ping, sessionId));
    }

    private static GatewayPacket Pong(GatewayPacket request, long sessionId) => new()
    { Type = GatewayPacketType.Pong, RequestId = request.RequestId, SessionId = sessionId };

    private static GatewayPacket Reply(GatewayPacket request, byte[]? payload = null) => new()
    {
        Type = GatewayPacketType.Response, RequestId = request.RequestId, Cmd = request.Cmd, Act = request.Act,
        SessionId = 123, UserId = 10001, Payload = payload
    };

    private static async Task<GatewayPacket> ReadPacket(Stream stream)
    {
        var prefix = new byte[4];
        await stream.ReadExactlyAsync(prefix);
        int length = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(prefix);
        var data = new byte[length];
        await stream.ReadExactlyAsync(data);
        return new GatewayPacketCodec().Decode(data);
    }

    private static async Task WritePacket(Stream stream, GatewayPacket packet)
    {
        byte[] data = new GatewayPacketCodec().Encode(packet);
        var prefix = new byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(prefix, data.Length);
        await stream.WriteAsync(prefix.AsMemory(0, 1));
        await stream.WriteAsync(prefix.AsMemory(1));
        await stream.WriteAsync(data.AsMemory(0, 7));
        await stream.WriteAsync(data.AsMemory(7));
    }
}
