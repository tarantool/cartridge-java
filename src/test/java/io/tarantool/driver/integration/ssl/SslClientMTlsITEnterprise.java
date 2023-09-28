package io.tarantool.driver.integration.ssl;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientBuilder;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.TarantoolImageParams;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Oleg Kuznetsov
 * @author Ivan Dneprov
 */
public class SslClientMTlsITEnterprise {

    private static final Logger log = LoggerFactory.getLogger(SslClientMTlsITEnterprise.class);

    private static TarantoolContainer containerWithSsl;
    private static final String RESOURCE_PATH = "org/testcontainers/containers/enterprise/ssl/mtls/";

    @BeforeAll
    public static void setUp() throws Exception {
        final File dockerfile = new File(
            SslClientITEnterprise.class.getClassLoader()
                .getResource("org/testcontainers/containers/enterprise/Dockerfile").toURI()
        );
        final Map<String, String> buildArgs = new HashMap<>();
        buildArgs.put("DOWNLOAD_SDK_URI", System.getenv("DOWNLOAD_SDK_URI"));
        buildArgs.put("SDK_VERSION", System.getenv("SDK_VERSION"));

        containerWithSsl = new TarantoolContainer(
            new TarantoolImageParams("tarantool-enterprise", dockerfile, buildArgs))
            .withScriptFileName("mtls_server.lua")
            .withUsername("test_user")
            .withPassword("test_password")
            .withMemtxMemory(256 * 1024 * 1024)
            .withDirectoryBinding(RESOURCE_PATH)
            .withLogConsumer(new Slf4jLogConsumer(log))
            .withSslContext(org.testcontainers.containers.SslContext.getSslContext(
                RESOURCE_PATH + "ca.key",
                RESOURCE_PATH + "ca.crt"));

        if (!containerWithSsl.isRunning()) {
            containerWithSsl.start();
        }
    }

    @Test
    public void test_clientConnectWithMtls_shouldWorkFine() throws Exception {
        //when
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clientWithSsl =
            TarantoolClientFactory.createClient()
                .withAddress(containerWithSsl.getHost(), containerWithSsl.getMappedPort(3301))
                .withCredentials("test_user", "test_password")
                .withSslContext(getSslContextWithCA())
                .build();

        //then
        assertDoesNotThrow(clientWithSsl::getVersion);
        final List<?> result = assertDoesNotThrow(() -> clientWithSsl.eval("return 'test'").join());
        assertEquals("test", result.get(0));
    }

    @Test
    public void test_clientWithoutCA_shouldThrowException_ifServerWithMTLS() throws Exception {
        //given
        final SslContext sslContext = SslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .build();
        //when
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clientWithSsl =
            TarantoolClientFactory.createClient()
                .withAddress(containerWithSsl.getHost(), containerWithSsl.getMappedPort(3301))
                .withCredentials("test_user", "test_password")
                .withSslContext(sslContext)
                .build();

        //then
        assertThrows(TarantoolClientException.class, clientWithSsl::getVersion);
        assertThrows(CompletionException.class, () -> clientWithSsl.eval("return 'test'").join());
    }

    private static SslContext getSslContextWithCA() throws Exception {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        final File keyCertChainFile = new File(classloader
            .getResource(RESOURCE_PATH + "ca.crt").toURI());
        final File keyFile = new File(classloader
            .getResource(RESOURCE_PATH + "ca.key").toURI());

        String keyStoreFilePassword = "12345678";
        KeyStore keyStore = KeyStore.getInstance("PKCS12");

        InputStream trustStore = classloader
            .getResourceAsStream(RESOURCE_PATH + "trustStoreFile");
        keyStore.load(trustStore, keyStoreFilePassword.toCharArray());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory
            .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        return SslContextBuilder.forClient()
            .trustManager(trustManagerFactory)
            .keyManager(keyCertChainFile, keyFile)
            .build();
    }
}
