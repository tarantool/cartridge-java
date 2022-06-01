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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.TarantoolImageParams;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.Security;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SslClientGOSTCypherITEnterprise {

    private static final Logger log = LoggerFactory.getLogger(SslClientITEnterprise.class);

    private static TarantoolContainer containerWithSsl;

    @BeforeAll
    public static void setUp() throws URISyntaxException, IOException {
        final File dockerfile = new File(
                SslClientITEnterprise.class.getClassLoader()
                        .getResource("org/testcontainers/containers/enterprise/Dockerfile").toURI()
        );
        final Map<String, String> buildArgs = new HashMap<>();
        buildArgs.put("DOWNLOAD_SDK_URI", System.getenv("DOWNLOAD_SDK_URI"));
        buildArgs.put("SDK_VERSION", "tarantool-enterprise-bundle-2.10.0-beta2-91-g08c9b4963-r474");

        final SslContext sslContext = getSslContext();
        final TarantoolClientBuilder tarantoolClientBuilder = TarantoolClientFactory.createClient()
                .withSslContext(sslContext)
                .withConnectTimeout(5000);

        containerWithSsl = new TarantoolContainer(
                new TarantoolImageParams("tarantool-enterprise", dockerfile, buildArgs), tarantoolClientBuilder)
                .withScriptFileName("ssl_server.lua")
                .withUsername("test_user")
                .withPassword("test_password")
                .withMemtxMemory(256 * 1024 * 1024)
                .withDirectoryBinding("org/testcontainers/containers/enterprise/ssl/cyphers_tests/gost")
                .withLogConsumer(new Slf4jLogConsumer(log));

        if (!containerWithSsl.isRunning()) {
            containerWithSsl.start();
        }
    }

    @Test
    public void test_should_clientConnectWithSsl() throws IOException {
        //when
        final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient =
                TarantoolClientFactory.createClient()
                        .withAddress(containerWithSsl.getHost(), containerWithSsl.getMappedPort(3301))
                        .withCredentials("test_user", "test_password")
                        .withSslContext(getSslContext())
                        .build();

        //then
        assertDoesNotThrow(tarantoolClient::getVersion);

        final List<?> result = assertDoesNotThrow(() -> tarantoolClient.eval("return 'test'").join());
        assertEquals("test", result.get(0));
    }

    @Test
    public void test_should_clientWithoutGOSTThrowException() throws Exception {
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
        assertThrows(TarantoolClientException.class, () -> clientWithSsl.eval("return 'test'").join());
    }

    @NotNull
    private static SslContext getSslContext() throws IOException {
        return SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .sslContextProvider(new ru.CryptoPro.ssl.Provider())
                .protocols("TLSv1.2")
                .build();
    }

    @AfterAll
    public static void afterAll() {
        Security.setProperty("ssl.KeyManagerFactory.algorithm", "SunX509");
        Security.setProperty("ssl.TrustManagerFactory.algorithm", "SunX509");
    }
}
