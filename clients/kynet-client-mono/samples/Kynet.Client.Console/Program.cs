using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Kynet.Client;
using ProtoBuf;

string endpoint = args.Length > 0 ? args[0] : "tcp://127.0.0.1:30001";
bool verify = Array.IndexOf(args, "--verify") >= 0;
using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
CancellationToken token = deadline.Token;
NetClient CreateClient() => new(new NetClientOptions
{
    Endpoint = endpoint,
    HeartbeatInterval = TimeSpan.FromMilliseconds(250),
    HeartbeatTimeout = TimeSpan.FromSeconds(2)
});

using var client = CreateClient();
client.StateChanged += state => Console.WriteLine("state=" + state);
client.Error += exception => Console.WriteLine("connection: " + exception.Message);
var welcome = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
using var welcomeSubscription = client.Subscribe<StringValue>(5, 6, message => welcome.TrySetResult(message.Value));
await client.ConnectAsync(token);
Console.WriteLine("session=" + client.Session.SessionId);
var denied = await client.RequestAsync<string>(1, 2, cancellationToken: token);
Check(denied.ErrorCode == "UNAUTHORIZED", "protected action requires login");
var invalid = await client.RequestAsync<LoginMessage>(1, 1,
    new LoginMessage { Username = "mono", Password = "invalid" }, cancellationToken: token);
Check(invalid.ErrorCode == "LOGIN_FAILED", "login errors preserve server error code");
var login = new LoginMessage { Username = "mono", Password = "123456" };
var response = await client.RequestAsync<LoginMessage>(1, 1, login, cancellationToken: token);
Console.WriteLine("token=" + response.EnsureSuccess().Token);
Check(client.Session.UserId == 10001, "server updates authenticated user");
Check((await welcome.Task.WaitAsync(token)).Contains("mono"), "Google StringValue welcome push");
var hello = await client.RequestAsync<string>(1, 2, cancellationToken: token);
Console.WriteLine(hello.EnsureSuccess());
Check(hello.Data.Contains("attachment.username=mono"), "server retains attachment between actions");

var broadcastDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
int broadcasts = 0;
using var broadcastsSubscription = client.Subscribe(200001, _ =>
{
    if (Interlocked.Increment(ref broadcasts) >= 6) broadcastDone.TrySetResult(true);
});
(await client.RequestAsync<string>(1, 5, cancellationToken: token)).EnsureSuccess();
await broadcastDone.Task.WaitAsync(token);
Console.WriteLine("broadcasts=" + broadcasts);
await client.SendOneWayAsync(1, 3, login, token);

if (verify)
{
    var disconnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    client.StateChanged += state => { if (state == ClientState.Disconnected) disconnected.TrySetResult(true); };
    using var second = CreateClient();
    await second.ConnectAsync(token);
    (await second.RequestAsync<LoginMessage>(1, 1, login, cancellationToken: token)).EnsureSuccess();
    await disconnected.Task.WaitAsync(token);
    Check(client.Session.SessionId == 0, "kicked connection clears its local session");
    (await second.RequestAsync<string>(1, 4, cancellationToken: token)).EnsureSuccess();
    var afterLogout = await second.RequestAsync<string>(1, 2, cancellationToken: token);
    Check(afterLogout.ErrorCode == "UNAUTHORIZED" && second.Session.UserId == 0, "logout clears authentication");
    await second.DisconnectAsync();
    Console.WriteLine("INTEROP PASS " + endpoint);
}
else
{
    (await client.RequestAsync<string>(1, 4, cancellationToken: token)).EnsureSuccess();
}
await client.DisconnectAsync();

static void Check(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException("Verification failed: " + message);
}

// Field numbers match examples/example-login-service/.../game/message/LoginMessage.java.
[ProtoContract]
public sealed class LoginMessage
{
    [ProtoMember(1)] public string Username { get; set; } = "";
    [ProtoMember(2)] public string Password { get; set; } = "";
    [ProtoMember(3)] public string Token { get; set; } = "";
}
