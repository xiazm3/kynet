package cn.itsub.kynet.framework.gateway;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class GatewayTlsConfig {
    private final Path certificateChain;
    private final Path privateKey;
    private final String privateKeyPassword;

    public GatewayTlsConfig(Path certificateChain, Path privateKey, String privateKeyPassword) {
        if (certificateChain == null || privateKey == null) {
            throw new IllegalArgumentException("certificateChain and privateKey are required");
        }
        this.certificateChain = certificateChain;
        this.privateKey = privateKey;
        this.privateKeyPassword = privateKeyPassword;
    }

    public static GatewayTlsConfig of(String certificateChain, String privateKey) {
        return of(certificateChain, privateKey, null);
    }

    public static GatewayTlsConfig of(String certificateChain, String privateKey, String privateKeyPassword) {
        return new GatewayTlsConfig(Path.of(certificateChain), Path.of(privateKey), privateKeyPassword);
    }

    public SslContext buildServerContext() {
        if (!Files.isRegularFile(certificateChain)) {
            throw new IllegalStateException("Gateway TLS certificate chain does not exist: " + certificateChain);
        }
        if (!Files.isRegularFile(privateKey)) {
            throw new IllegalStateException("Gateway TLS private key does not exist: " + privateKey);
        }
        try {
            return SslContextBuilder.forServer(
                    certificateChain.toFile(),
                    privateKey.toFile(),
                    privateKeyPassword)
                    .protocols("TLSv1.3", "TLSv1.2")
                    .build();
        } catch (SSLException e) {
            throw new IllegalStateException("Failed to create Gateway TLS context", e);
        }
    }

    @Override
    public String toString() {
        return "GatewayTlsConfig[certificateChain=" + certificateChain + ", privateKey=" + privateKey + "]";
    }
}
