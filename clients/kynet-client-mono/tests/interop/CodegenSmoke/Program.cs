using System;
using System.Threading.Tasks;
using Kynet.Client;
using Kynet.Client.Generated;

// 端到端冒烟：用 kynet-codegen 生成的 SignActionProxy 调真实服务器。
// 覆盖：消息返回 / int→int 返回 / List<string> 返回 / 回调风格 / GetValue 别名 / Log。
internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        string endpoint = args.Length > 0 ? args[0] : "tcp://127.0.0.1:30001";
        using var client = new NetClient(new NetClientOptions
        {
            Endpoint = endpoint,
            HeartbeatInterval = TimeSpan.FromMilliseconds(250),
            HeartbeatTimeout = TimeSpan.FromSeconds(2)
        });
        KynetGeneratedRuntime.DefaultClient = client;
        await client.ConnectAsync();

        var signResponse = await SignActionProxy.SignAsync(new SignTokenMessage { Ticket = "abc" });
        signResponse.Log();
        var sign = signResponse.EnsureSuccess();
        Check(sign.Day == 3, "sign day");
        Check(sign.Bonus == "签到成功：abc", "sign bonus");
        Check(signResponse.GetValue<SignResultMessage>().Day == 3, "GetValue alias");

        int next = (await SignActionProxy.NextDayAsync(2)).EnsureSuccess();
        Check(next == 3, "nextDay day+1");

        var rewards = (await SignActionProxy.RewardsAsync("kynet")).EnsureSuccess();
        Check(rewards.Count == 2 && rewards[0] == "kynet-首签礼包" && rewards[1] == "连续签到+500金币",
            "rewards values");

        var callbackDone = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        SignActionProxy.NextDay(9, result =>
        {
            callbackDone.TrySetResult(result.EnsureSuccess() == 10);
        });
        Check(await callbackDone.Task.WaitAsync(TimeSpan.FromSeconds(5)), "callback style nextDay");

        Console.WriteLine("CODEGEN SMOKE PASS " + endpoint);
        await client.DisconnectAsync();
        return 0;
    }

    private static void Check(bool condition, string name)
    {
        if (!condition) throw new InvalidOperationException("Smoke failed: " + name);
    }
}
