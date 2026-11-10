using System;
using System.Linq;
using System.Text;
using Google.Protobuf.WellKnownTypes;
using Kynet.Client.Codec;
using Kynet.Client.Protocol;
using ProtoBuf;
using Xunit;

namespace Kynet.Client.Tests;

public class ProtocolTests
{
    private readonly GatewayPacketCodec _codec = new();

    [Fact]
    public void RequestMatchesJavaWireLayout()
    {
        var packet = new GatewayPacket
        {
            Type = GatewayPacketType.Request, RequestId = 0x0102030405060708,
            Cmd = 1, Act = 2, TimeoutMillis = 3000, SessionId = long.MaxValue,
            UserId = 10001, Payload = new byte[] { 0x0a, 1, 0x61 }
        };
        Assert.Equal(
            "4B594E5400010000010203040506070800000001000000020000000000000BB8" +
            "7FFFFFFFFFFFFFFF0000000000002711FFFFFFFFFFFFFFFF000000030A0161",
            Convert.ToHexString(_codec.Encode(packet)));
        var decoded = _codec.Decode(_codec.Encode(packet));
        Assert.Equal(packet.SessionId, decoded.SessionId);
        Assert.Equal(packet.Payload, decoded.Payload);
    }

    [Fact]
    public void NullAndEmptyAreDistinct()
    {
        var packet = new GatewayPacket { Type = GatewayPacketType.Response };
        Assert.Null(_codec.Decode(_codec.Encode(packet)).Payload);
        packet.Payload = Array.Empty<byte>();
        packet.ErrorCode = "";
        var decoded = _codec.Decode(_codec.Encode(packet));
        Assert.Empty(decoded.Payload!);
        Assert.Equal("", decoded.ErrorCode);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(5, 2)]
    [InlineData(6, 5)]
    [InlineData(48, 0x7f)]
    [InlineData(48, 0x80)]
    public void RejectsMalformedPackets(int index, byte value)
    {
        var data = _codec.Encode(new GatewayPacket());
        data[index] = value;
        Assert.Throws<GatewayProtocolException>(() => _codec.Decode(data));
    }

    [Fact]
    public void RejectsEveryTruncationAndTrailingData()
    {
        var data = _codec.Encode(new GatewayPacket { ErrorMessage = "中文", Payload = new byte[12] });
        for (int length = 0; length < data.Length; length++)
            Assert.Throws<GatewayProtocolException>(() => _codec.Decode(data.Take(length).ToArray()));
        Assert.Throws<GatewayProtocolException>(() => _codec.Decode(data.Concat(new byte[1]).ToArray()));
    }

    [Fact]
    public void EnforcesUtf8ByteAndPayloadLimits()
    {
        var codec = new GatewayPacketCodec(128, 2, 3);
        Assert.Throws<GatewayProtocolException>(() => codec.Encode(new GatewayPacket { Payload = new byte[3] }));
        Assert.Throws<GatewayProtocolException>(() => codec.Encode(new GatewayPacket { ErrorCode = "中文" }));
        var bytes = _codec.Encode(new GatewayPacket { Payload = new byte[3] });
        Assert.Throws<GatewayProtocolException>(() => codec.Decode(bytes));
    }

    [Fact]
    public void ActionKeysUseUnsignedHalves()
    {
        int key = ActionKey.Of(65535, 65535);
        Assert.Equal(-1, key);
        Assert.Equal(65535, ActionKey.CmdOf(key));
        Assert.Equal(65535, ActionKey.ActOf(key));
        Assert.Throws<ArgumentOutOfRangeException>(() => ActionKey.Of(65536, 1));
    }

    [Fact]
    public void PayloadMatchesBothJavaStringFormatsAndLoginContract()
    {
        var codec = new PayloadCodec();
        Assert.Equal(Encoding.UTF8.GetBytes("hello"), codec.Encode("hello"));
        Assert.Equal("hello", codec.Decode<string>(Encoding.UTF8.GetBytes("hello")));
        Assert.Equal("welcome", codec.Decode<StringValue>(Convert.FromHexString("0A0777656C636F6D65")).Value);
        var login = new LoginMessage { Username = "a", Password = "b", Token = "c" };
        Assert.Equal("0A01611201621A0163", Convert.ToHexString(codec.Encode(login)!));
        Assert.Equal("c", codec.Decode<LoginMessage>(codec.Encode(login)).Token);
    }

    [Theory]
    [InlineData(0, "08001000")]
    [InlineData(42, "0800102A")]
    [InlineData(-1, "080010FFFFFFFFFFFFFFFFFF01")]
    public void PrimitiveIntUsesJavaPrimitiveValue(int value, string hex)
    {
        var codec = new PayloadCodec();
        Assert.Equal(hex, Convert.ToHexString(codec.Encode(value)!));
        Assert.Equal(value, codec.Decode<int>(Convert.FromHexString(hex)));
    }

    [Fact]
    public void OtherPrimitivesRoundTripAndRejectMismatchedKind()
    {
        var codec = new PayloadCodec();
        Assert.Equal(long.MinValue, codec.Decode<long>(codec.Encode(long.MinValue)));
        Assert.False(codec.Decode<bool>(codec.Encode(false)));
        Assert.True(codec.Decode<bool>(codec.Encode(true)));
        Assert.Equal(1.25d, codec.Decode<double>(codec.Encode(1.25d)));
        Assert.Equal(-2.5f, codec.Decode<float>(codec.Encode(-2.5f)));
        Assert.Throws<System.IO.InvalidDataException>(() => codec.Decode<int>(codec.Encode(42L)));
    }

    [ProtoContract]
    public sealed class LoginMessage
    {
        [ProtoMember(1)] public string Username { get; set; } = "";
        [ProtoMember(2)] public string Password { get; set; } = "";
        [ProtoMember(3)] public string Token { get; set; } = "";
    }
}
