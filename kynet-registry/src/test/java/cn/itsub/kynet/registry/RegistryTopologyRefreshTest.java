package cn.itsub.kynet.registry;

import cn.itsub.kynet.common.codec.FrameCodec;
import cn.itsub.kynet.common.codec.ProtobufPayloadCodec;
import cn.itsub.kynet.common.protocol.MessageType;
import cn.itsub.kynet.common.protocol.RegisterServiceRequest;
import cn.itsub.kynet.common.protocol.WireMessage;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryTopologyRefreshTest {
    @Test @Timeout(10) void periodicallyResendsTopologyOnTheRegistrationConnection() throws Exception {
        String oldCluster = System.getProperty("kynet.registry.cluster.enabled");
        String oldRefresh = System.getProperty("kynet.registry.topologyRefreshMs");
        System.setProperty("kynet.registry.cluster.enabled", "false");
        System.setProperty("kynet.registry.topologyRefreshMs", "1000");
        try {
            int port;
            try (ServerSocket reserved = new ServerSocket(0)) { port = reserved.getLocalPort(); }
            RegistryServer server = new RegistryServer(port);
            server.start();
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(4_000);
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                DataInputStream input = new DataInputStream(socket.getInputStream());

                RegisterServiceRequest request = new RegisterServiceRequest();
                request.setServiceName("TopologyProbe");
                request.setServiceId(1);
                request.setInstanceId(42);
                request.setInstanceGeneration(1);
                request.setDirectChannel("aeron:udp?endpoint=127.0.0.1:40000");
                request.setDirectStreamId(1010);
                request.setIncarnation(1);
                WireMessage register = new WireMessage();
                register.setMessageType(MessageType.REGISTER_SERVICE);
                register.setSourceInstanceId(42);
                register.setPayloadType(RegisterServiceRequest.class.getName());
                register.setPayload(ProtobufPayloadCodec.encode(request));
                UnsafeBuffer encoded = new UnsafeBuffer(new byte[1024]);
                int length = FrameCodec.encode(register, encoded, 0);
                byte[] bytes = new byte[length];
                encoded.getBytes(0, bytes);
                output.writeInt(length);
                output.write(bytes);
                output.flush();

                int received = 0;
                long revision = -1;
                while (received < 2) {
                    int frameLength = input.readInt();
                    assertTrue(frameLength > 0 && frameLength < RegistryServer.MAX_FRAME_BYTES);
                    byte[] frameBytes = new byte[frameLength];
                    input.readFully(frameBytes);
                    FrameCodec.DecodedFrame frame = FrameCodec.decodeView(new UnsafeBuffer(frameBytes), 0, frameLength);
                    if (frame.messageType() != MessageType.REGISTRY_TOPOLOGY) continue;
                    assertTrue(frame.payloadLength() > 0);
                    if (received == 0) revision = frame.requestId();
                    else assertEquals(revision, frame.requestId());
                    received++;
                }
            } finally {
                server.stop();
            }
        } finally {
            restore("kynet.registry.cluster.enabled", oldCluster);
            restore("kynet.registry.topologyRefreshMs", oldRefresh);
        }
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
