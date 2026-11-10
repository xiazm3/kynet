using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Kynet.Client.Protocol;
using Kynet.Client.Transport;

namespace Kynet.Client
{
    public sealed class NetClient : IDisposable
    {
        private readonly object _sync = new object();
        private readonly NetClientOptions _options;
        private readonly GatewayPacketCodec _packetCodec;
        private readonly SemaphoreSlim _writeGate = new SemaphoreSlim(1, 1);
        private readonly Dictionary<long, PendingRequest> _pending = new Dictionary<long, PendingRequest>();
        private readonly Dictionary<int, List<Action<GatewayPacket>>> _listeners = new Dictionary<int, List<Action<GatewayPacket>>>();
        private CancellationTokenSource? _lifetime;
        private Task? _runner;
        private TaskCompletionSource<bool>? _ready;
        private WireConnection? _connection;
        private ClientSession _session = new ClientSession();
        private ClientState _state;
        private long _requestId;
        private int _pendingWrites;
        private bool _stopping;
        private bool _disposed;

        public event Action<ClientState>? StateChanged;
        public event Action<ClientSession>? SessionChanged;
        public event Action<GatewayPacket>? PushReceived;
        public event Action<Exception>? Error;
        public ClientState State { get { lock (_sync) return _state; } }
        public ClientSession Session { get { lock (_sync) return _session; } }
        public int PendingRequestCount { get { lock (_sync) return _pending.Count; } }

        public NetClient(NetClientOptions? options = null)
        {
            _options = (options ?? new NetClientOptions()).Snapshot();
            _packetCodec = _options.CreatePacketCodec();
        }

        public async Task ConnectAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Task ready;
            lock (_sync)
            {
                ThrowIfDisposed();
                if (_stopping) throw new InvalidOperationException("Disconnect is still in progress.");
                if (_runner == null || _runner.IsCompleted)
                {
                    _lifetime?.Dispose();
                    _lifetime = new CancellationTokenSource();
                    _ready = NewCompletion<bool>();
                    CancellationToken lifetimeToken = _lifetime.Token;
                    _runner = Task.Run(() => RunAsync(lifetimeToken));
                }
                ready = _ready!.Task;
            }
            // Caller cancellation stops waiting; DisconnectAsync stops the shared connection.
            await WithCancellationAsync(ready, cancellationToken).ConfigureAwait(false);
        }

        public async Task DisconnectAsync()
        {
            Task? runner;
            lock (_sync)
            {
                _stopping = true;
                _lifetime?.Cancel();
                _connection?.Dispose();
                runner = _runner;
            }
            try { if (runner != null) await runner.ConfigureAwait(false); }
            finally { lock (_sync) _stopping = false; }
        }

        public async Task<ActionResponse<T>> RequestAsync<T>(int cmd, int act, object? payload = null,
            TimeSpan? timeout = null, CancellationToken cancellationToken = default)
        {
            ValidateAction(cmd, act);
            WireConnection connection = GetConnection();
            var packet = NewRequest(cmd, act, payload);
            GatewayPacket response = await RoundTripAsync(connection, packet, GatewayPacketType.Response,
                timeout ?? _options.RequestTimeout, cancellationToken).ConfigureAwait(false);
            return new ActionResponse<T>(response, _options.PayloadCodec);
        }

        public Task<ActionResponse<T>> RequestAsync<T>(int messageId, object? payload = null,
            TimeSpan? timeout = null, CancellationToken cancellationToken = default) =>
            RequestAsync<T>(ActionKey.CmdOf(messageId), ActionKey.ActOf(messageId), payload, timeout, cancellationToken);

        public async Task SendOneWayAsync(int cmd, int act, object? payload = null, CancellationToken cancellationToken = default)
        {
            ValidateAction(cmd, act);
            WireConnection connection = GetConnection();
            var packet = NewRequest(cmd, act, payload);
            packet.OneWay = true;
            using (var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                deadline.CancelAfter(_options.RequestTimeout);
                await SendAsync(connection, packet, deadline.Token).ConfigureAwait(false);
            }
        }

        public Task SendOneWayAsync(int messageId, object? payload = null, CancellationToken cancellationToken = default) =>
            SendOneWayAsync(ActionKey.CmdOf(messageId), ActionKey.ActOf(messageId), payload, cancellationToken);

        public async Task<ClientSession> PingAsync(CancellationToken cancellationToken = default)
        {
            await PingAsync(GetConnection(), _options.HeartbeatTimeout, cancellationToken).ConfigureAwait(false);
            return Session;
        }

        public IDisposable Subscribe(int cmd, int act, Action<GatewayPacket> handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            int key = ActionKey.Of(cmd, act);
            lock (_sync)
            {
                ThrowIfDisposed();
                if (!_listeners.TryGetValue(key, out var list)) _listeners[key] = list = new List<Action<GatewayPacket>>();
                list.Add(handler);
            }
            return new Subscription(() =>
            {
                lock (_sync)
                    if (_listeners.TryGetValue(key, out var list))
                    {
                        list.Remove(handler);
                        if (list.Count == 0) _listeners.Remove(key);
                    }
            });
        }

        public IDisposable Subscribe(int messageId, Action<GatewayPacket> handler) =>
            Subscribe(ActionKey.CmdOf(messageId), ActionKey.ActOf(messageId), handler);

        public IDisposable Subscribe<T>(int cmd, int act, Action<T> handler)
        {
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            return Subscribe(cmd, act, packet => handler(_options.PayloadCodec.Decode<T>(packet.Payload)));
        }

        public IDisposable Subscribe<T>(int messageId, Action<T> handler) =>
            Subscribe(ActionKey.CmdOf(messageId), ActionKey.ActOf(messageId), handler);

        private async Task RunAsync(CancellationToken lifetimeToken)
        {
            bool connectedOnce = false;
            double delay = _options.ReconnectInitialDelay.TotalMilliseconds;
            try
            {
                while (!lifetimeToken.IsCancellationRequested)
                {
                    SetState(connectedOnce ? ClientState.Reconnecting : ClientState.Connecting);
                    using (var connection = new WireConnection(_options.GetEndpoint(), _packetCodec))
                    using (var connectionStop = CancellationTokenSource.CreateLinkedTokenSource(lifetimeToken))
                    using (connectionStop.Token.Register(connection.Dispose))
                    {
                        Task? receiver = null;
                        Task? heartbeat = null;
                        lock (_sync) _connection = connection;
                        try
                        {
                            using (var connectDeadline = CancellationTokenSource.CreateLinkedTokenSource(connectionStop.Token))
                            {
                                connectDeadline.CancelAfter(_options.ConnectTimeout);
                                await connection.ConnectAsync(connectDeadline.Token).ConfigureAwait(false);
                            }
                            receiver = ReceiveLoopAsync(connection, connectionStop);
                            await PingAsync(connection, _options.ConnectTimeout, connectionStop.Token).ConfigureAwait(false);
                            SetState(ClientState.Recovering);
                            if (connectedOnce && _options.SessionRecoveryHandler != null)
                                await _options.SessionRecoveryHandler(this, connectionStop.Token).ConfigureAwait(false);
                            connectionStop.Token.ThrowIfCancellationRequested();
                            connectedOnce = true;
                            delay = _options.ReconnectInitialDelay.TotalMilliseconds;
                            SetState(ClientState.Connected);
                            lock (_sync) _ready!.TrySetResult(true);
                            heartbeat = HeartbeatLoopAsync(connection, connectionStop);
                            await receiver.ConfigureAwait(false);
                        }
                        catch (Exception exception)
                        {
                            if (!lifetimeToken.IsCancellationRequested) ReportError(exception);
                            if (!connectedOnce)
                            {
                                lock (_sync) _ready!.TrySetException(exception);
                                break;
                            }
                        }
                        finally
                        {
                            connectionStop.Cancel();
                            connection.Dispose();
                            lock (_sync)
                            {
                                if (ReferenceEquals(_connection, connection)) _connection = null;
                                FailPendingLocked(connection, new GatewayDisconnectedException("Gateway connection closed."));
                                if (connectedOnce && _options.AutoReconnect && !lifetimeToken.IsCancellationRequested
                                    && _ready!.Task.IsCompleted) _ready = NewCompletion<bool>();
                            }
                            if (receiver != null) await ObserveAsync(receiver).ConfigureAwait(false);
                            if (heartbeat != null) await ObserveAsync(heartbeat).ConfigureAwait(false);
                            UpdateSession(new ClientSession());
                        }
                    }
                    if (!_options.AutoReconnect || lifetimeToken.IsCancellationRequested) break;
                    SetState(ClientState.Reconnecting);
                    await Task.Delay(TimeSpan.FromMilliseconds(delay), lifetimeToken).ConfigureAwait(false);
                    delay = Math.Min(delay * 2, _options.ReconnectMaxDelay.TotalMilliseconds);
                }
            }
            catch (OperationCanceledException) when (lifetimeToken.IsCancellationRequested) { }
            finally
            {
                lock (_sync) _ready!.TrySetCanceled();
                SetState(_disposed ? ClientState.Closed : ClientState.Disconnected);
            }
        }

        private async Task ReceiveLoopAsync(WireConnection connection, CancellationTokenSource stop)
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    GatewayPacket packet = await connection.ReceiveAsync(stop.Token).ConfigureAwait(false);
                    if (packet.Type == GatewayPacketType.Push)
                    {
                        // Direct user-targeted pushes can carry sessionId=0; they are routing
                        // metadata, not an authoritative snapshot of this connection's login state.
                        Action<GatewayPacket>[] handlers;
                        lock (_sync)
                            handlers = _listeners.TryGetValue(ActionKey.Of(packet.Cmd, packet.Act), out var list)
                                ? list.ToArray() : Array.Empty<Action<GatewayPacket>>();
                        foreach (var handler in handlers) Dispatch(() => handler(packet));
                        Notify(PushReceived, packet);
                        continue;
                    }
                    if (packet.Type != GatewayPacketType.Response && packet.Type != GatewayPacketType.Pong)
                        throw new GatewayProtocolException("Unexpected inbound packet type: " + packet.Type);
                    ClientSession currentSession = Session;
                    if (currentSession.SessionId > 0 && packet.SessionId != currentSession.SessionId)
                        throw new GatewayDisconnectedException("Gateway replaced the session; login must be recovered.");
                    PendingRequest? pending;
                    lock (_sync)
                    {
                        if (!_pending.TryGetValue(packet.RequestId, out pending) || !ReferenceEquals(pending.Connection, connection)) continue;
                        if (packet.Type != pending.ResponseType || packet.Cmd != pending.Cmd || packet.Act != pending.Act)
                            throw new GatewayProtocolException("Response does not match the pending request.");
                        _pending.Remove(packet.RequestId);
                    }
                    UpdateSession(new ClientSession(packet.SessionId, packet.UserId));
                    pending.Completion.TrySetResult(packet);
                }
            }
            catch (Exception exception)
            {
                lock (_sync) FailPendingLocked(connection, new GatewayDisconnectedException("Gateway receive failed.", exception));
                throw;
            }
            finally { stop.Cancel(); }
        }

        private async Task HeartbeatLoopAsync(WireConnection connection, CancellationTokenSource stop)
        {
            if (_options.HeartbeatInterval == TimeSpan.Zero) return;
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    await Task.Delay(_options.HeartbeatInterval, stop.Token).ConfigureAwait(false);
                    await PingAsync(connection, _options.HeartbeatTimeout, stop.Token).ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                if (!stop.IsCancellationRequested) ReportError(exception);
                stop.Cancel();
            }
        }

        private Task<GatewayPacket> PingAsync(WireConnection connection, TimeSpan timeout, CancellationToken cancellationToken) =>
            RoundTripAsync(connection, new GatewayPacket { Type = GatewayPacketType.Ping }, GatewayPacketType.Pong, timeout, cancellationToken);

        private async Task<GatewayPacket> RoundTripAsync(WireConnection connection, GatewayPacket packet,
            GatewayPacketType responseType, TimeSpan timeout, CancellationToken cancellationToken)
        {
            NetClientOptions.ValidateTimeout(timeout, nameof(timeout));
            cancellationToken.ThrowIfCancellationRequested();
            packet.RequestId = Interlocked.Increment(ref _requestId);
            packet.TimeoutMillis = (long)Math.Ceiling(timeout.TotalMilliseconds);
            var pending = new PendingRequest(connection, responseType, packet.Cmd, packet.Act);
            lock (_sync)
            {
                ThrowIfDisposed();
                if (!ReferenceEquals(_connection, connection)) throw new GatewayDisconnectedException("Connection changed before sending.");
                if (_pending.Count >= _options.MaxPendingRequests) throw new InvalidOperationException("Maximum pending requests reached.");
                _pending.Add(packet.RequestId, pending);
            }
            try
            {
                using (var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    deadline.CancelAfter(timeout);
                    using (deadline.Token.Register(() =>
                    {
                        if (cancellationToken.IsCancellationRequested) pending.Completion.TrySetCanceled();
                        else pending.Completion.TrySetException(new TimeoutException("Gateway request timed out: " + packet.RequestId));
                    }))
                    {
                        try { await SendAsync(connection, packet, deadline.Token).ConfigureAwait(false); }
                        catch (Exception exception) { pending.Completion.TrySetException(exception); }
                        return await pending.Completion.Task.ConfigureAwait(false);
                    }
                }
            }
            finally { lock (_sync) _pending.Remove(packet.RequestId); }
        }

        private async Task SendAsync(WireConnection connection, GatewayPacket packet, CancellationToken cancellationToken)
        {
            ClientSession session = Session;
            packet.SessionId = session.SessionId;
            packet.UserId = session.UserId;
            byte[] bytes = _packetCodec.Encode(packet);
            if (Interlocked.Increment(ref _pendingWrites) > _options.MaxPendingWrites)
            {
                Interlocked.Decrement(ref _pendingWrites);
                throw new InvalidOperationException("Maximum pending writes reached.");
            }
            try
            {
                await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    lock (_sync)
                        if (!ReferenceEquals(connection, _connection)) throw new GatewayDisconnectedException("Connection changed before writing.");
                    await connection.SendAsync(bytes, cancellationToken).ConfigureAwait(false);
                }
                finally { _writeGate.Release(); }
            }
            finally { Interlocked.Decrement(ref _pendingWrites); }
        }

        private GatewayPacket NewRequest(int cmd, int act, object? payload) => new GatewayPacket
        {
            Type = GatewayPacketType.Request, Cmd = cmd, Act = act,
            RequestId = Interlocked.Increment(ref _requestId),
            TimeoutMillis = (long)_options.RequestTimeout.TotalMilliseconds,
            Payload = _options.PayloadCodec.Encode(payload)
        };

        private WireConnection GetConnection()
        {
            lock (_sync)
            {
                ThrowIfDisposed();
                if (_connection == null || (_state != ClientState.Connected && _state != ClientState.Recovering))
                    throw new GatewayDisconnectedException("Gateway client is not ready: " + _state);
                return _connection;
            }
        }

        private static void ValidateAction(int cmd, int act)
        {
            ActionKey.Of(cmd, act);
            if (cmd == 0 || act == 0) throw new ArgumentException("Request cmd and act must be positive.");
        }

        private void FailPendingLocked(WireConnection connection, Exception exception)
        {
            var removed = new List<long>();
            foreach (var pair in _pending)
                if (ReferenceEquals(pair.Value.Connection, connection))
                {
                    pair.Value.Completion.TrySetException(exception);
                    removed.Add(pair.Key);
                }
            foreach (long id in removed) _pending.Remove(id);
        }

        private void UpdateSession(ClientSession session)
        {
            lock (_sync)
            {
                if (_session.SessionId == session.SessionId && _session.UserId == session.UserId) return;
                _session = session;
            }
            Notify(SessionChanged, session);
        }

        private void SetState(ClientState state)
        {
            lock (_sync)
            {
                if (_disposed) state = ClientState.Closed;
                if (_state == state) return;
                _state = state;
            }
            Notify(StateChanged, state);
        }

        private void Notify<T>(Action<T>? handlers, T value)
        {
            if (handlers == null) return;
            foreach (Action<T> handler in handlers.GetInvocationList()) Dispatch(() => handler(value));
        }

        private void Dispatch(Action action)
        {
            void Invoke() { try { action(); } catch (Exception exception) { ReportError(exception); } }
            if (_options.CallbackContext == null) Invoke();
            else _options.CallbackContext.Post(_ => Invoke(), null);
        }

        private void ReportError(Exception exception)
        {
            void Invoke()
            {
                var handlers = Error;
                if (handlers == null) return;
                foreach (Action<Exception> handler in handlers.GetInvocationList())
                    try { handler(exception); } catch { }
            }
            if (_options.CallbackContext == null) Invoke();
            else _options.CallbackContext.Post(_ => Invoke(), null);
        }

        private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(NetClient)); }
        private static TaskCompletionSource<T> NewCompletion<T>() => new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        private static async Task ObserveAsync(Task task) { try { await task.ConfigureAwait(false); } catch { } }

        private static async Task WithCancellationAsync(Task task, CancellationToken cancellationToken)
        {
            var canceled = NewCompletion<bool>();
            using (cancellationToken.Register(() => canceled.TrySetCanceled()))
                await (await Task.WhenAny(task, canceled.Task).ConfigureAwait(false)).ConfigureAwait(false);
        }

        public void Dispose()
        {
            lock (_sync)
            {
                if (_disposed) return;
                _disposed = true;
                _lifetime?.Cancel();
                _connection?.Dispose();
                if (_connection != null) FailPendingLocked(_connection, new ObjectDisposedException(nameof(NetClient)));
                _listeners.Clear();
            }
            UpdateSession(new ClientSession());
            SetState(ClientState.Closed);
        }

        private sealed class PendingRequest
        {
            public WireConnection Connection { get; }
            public GatewayPacketType ResponseType { get; }
            public int Cmd { get; }
            public int Act { get; }
            public TaskCompletionSource<GatewayPacket> Completion { get; } = NewCompletion<GatewayPacket>();
            public PendingRequest(WireConnection connection, GatewayPacketType responseType, int cmd, int act)
            { Connection = connection; ResponseType = responseType; Cmd = cmd; Act = act; }
        }

        private sealed class Subscription : IDisposable
        {
            private Action? _remove;
            public Subscription(Action remove) { _remove = remove; }
            public void Dispose() => Interlocked.Exchange(ref _remove, null)?.Invoke();
        }
    }
}
