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
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.TarantoolImageParams;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import javax.net.ssl.SSLException;
import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * @author Oleg Kuznetsov
 */
public class SslClientITEnterprise {

    private static final Logger log = LoggerFactory.getLogger(SslClientITEnterprise.class);

    private static TarantoolContainer containerWithSsl;
    private static TarantoolContainer containerWithoutSsl;

    @BeforeAll
    public static void setUp() throws URISyntaxException, SSLException {
        final File dockerfile = new File(
            SslClientITEnterprise.class.getClassLoader()
                .getResource("org/testcontainers/containers/enterprise/Dockerfile").toURI()
        );
        final Map<String, String> buildArgs = new HashMap<>();
        buildArgs.put("DOWNLOAD_SDK_URI", System.getenv("DOWNLOAD_SDK_URI"));
        buildArgs.put("SDK_VERSION", System.getenv("SDK_VERSION"));

        final TarantoolClientBuilder tarantoolClientBuilder = TarantoolClientFactory.createClient()
            .withSslContext(getSslContext());

        containerWithSsl = new TarantoolContainer(
            new TarantoolImageParams("tarantool-enterprise", dockerfile, buildArgs), tarantoolClientBuilder)
            .withScriptFileName("ssl_server.lua")
            .withUsername("test_user")
            .withPassword("test_password")
            .withMemtxMemory(256 * 1024 * 1024)
            .withDirectoryBinding("org/testcontainers/containers/enterprise/ssl")
            .withLogConsumer(new Slf4jLogConsumer(log));

        containerWithoutSsl = new TarantoolContainer(
            new TarantoolImageParams("tarantool-enterprise", dockerfile, buildArgs))
            .withScriptFileName("server.lua")
            .withUsername("test_user")
            .withPassword("test_password")
            .withMemtxMemory(256 * 1024 * 1024)
            .withDirectoryBinding("org/testcontainers/containers/enterprise")
            .withLogConsumer(new Slf4jLogConsumer(log));

        if (!containerWithSsl.isRunning()) {
            containerWithSsl.start();
        }

        if (!containerWithoutSsl.isRunning()) {
            containerWithoutSsl.start();
        }
    }

    @Test
    public void test_clientWithSsl_shouldWork_ifServerWithoutSSL() throws SSLException {
        //when
        final TarantoolClientBuilder tarantoolClientBuilder = TarantoolClientFactory.createClient()
            .withAddress(containerWithoutSsl.getHost(), containerWithoutSsl.getMappedPort(3301))
            .withCredentials("test_user", "test_password")
            .withSslContext(getSslContext());

        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clientWithSsl =
            tarantoolClientBuilder.build();
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
            tarantoolClientBuilder.withSecure(false).build();

        //then
        assertDoesNotThrow(client::getVersion);
        assertThrows(TarantoolClientException.class, clientWithSsl::getVersion);
    }

    @Test
    public void test_clientWithSsl_shouldWork() throws SSLException {
        //when
        final TarantoolClientBuilder tarantoolClientBuilder = TarantoolClientFactory.createClient()
            .withAddress(containerWithSsl.getHost(), containerWithSsl.getMappedPort(3301))
            .withCredentials("test_user", "test_password")
            .withSslContext(getSslContext());

        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> clientWithSsl =
            tarantoolClientBuilder.build();
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
            tarantoolClientBuilder.withSecure(false).build();

        //then
        assertDoesNotThrow(clientWithSsl::getVersion);
        assertThrows(TarantoolClientException.class, client::getVersion);

        final List<?> result = assertDoesNotThrow(() -> clientWithSsl.eval("return 'test'").join());
        assertEquals("test", result.get(0));
    }

    @NotNull
    private static SslContext getSslContext() throws SSLException {
        return SslContextBuilder.forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .build();
    }
}
