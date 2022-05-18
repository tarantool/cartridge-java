package io.tarantool.driver.integration;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.exceptions.TarantoolClientException;
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

    private static TarantoolContainer container;

    @BeforeAll
    public static void setUp() throws URISyntaxException {
        final File dockerfile = new File(
                SslClientITEnterprise.class.getClassLoader()
                        .getResource("org/testcontainers/containers/enterprise/Dockerfile").toURI()
        );
        final Map<String, String> buildArgs = new HashMap<>();
        buildArgs.put("DOWNLOAD_SDK_URI", System.getenv("DOWNLOAD_SDK_URI"));
        buildArgs.put("SDK_VERSION", "tarantool-enterprise-bundle-2.10.0-beta2-91-g08c9b4963-r474");

        container = new TarantoolContainer(
                new TarantoolImageParams("tarantool-enterprise", dockerfile, buildArgs))
                .withScriptFileName("ssl_server.lua")
                .withDirectoryBinding("org/testcontainers/containers/enterprise/ssl")
                .withLogConsumer(new Slf4jLogConsumer(log));

        if (!container.isRunning()) {
            container.start();
        }
    }

    @Test
    public void test_should_throwException_ifClientWithSslButServerNot() throws SSLException {
        final TarantoolServerAddress address =
                new TarantoolServerAddress("127.0.0.1", container.getMappedPort(3301));
        final SimpleTarantoolCredentials credentials =
                new SimpleTarantoolCredentials("test_user", "test_password");

        final ClusterTarantoolTupleClient clientWithSsl = makeClientWithSsl(address, credentials);
        final ClusterTarantoolTupleClient clientWithoutSsl = makeClient(address, credentials);

        assertDoesNotThrow(clientWithoutSsl::getVersion);
        assertThrows(TarantoolClientException.class, clientWithSsl::getVersion);
    }

    @Test
    public void test_should_clientConnectWithSsl() throws SSLException {
        final TarantoolServerAddress address =
                new TarantoolServerAddress("127.0.0.1", container.getMappedPort(3301));
        final SimpleTarantoolCredentials credentials =
                new SimpleTarantoolCredentials("test_user", "test_password");

        final ClusterTarantoolTupleClient clientWithSsl = makeClientWithSsl(address, credentials);
        final ClusterTarantoolTupleClient clientWithoutSsl = makeClient(address, credentials);

        assertDoesNotThrow(clientWithSsl::getVersion);
        assertThrows(TarantoolClientException.class, clientWithoutSsl::getVersion);

        final List<?> result = assertDoesNotThrow(() -> clientWithSsl.eval("return 'test'").join());
        assertEquals("test", result.get(0));
    }

    private ClusterTarantoolTupleClient makeClientWithSsl(
            TarantoolServerAddress address, SimpleTarantoolCredentials credentials) throws SSLException {
        final SslContext sslContext = SslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();

        final TarantoolClientConfig tarantoolClientConfig = new TarantoolClientConfig();
        tarantoolClientConfig.setSslContext(sslContext);
        tarantoolClientConfig.setCredentials(credentials);

        return new ClusterTarantoolTupleClient(tarantoolClientConfig, address);
    }

    private ClusterTarantoolTupleClient makeClient(
            TarantoolServerAddress address, SimpleTarantoolCredentials credentials) {
        return new ClusterTarantoolTupleClient(
                TarantoolClientConfig.builder()
                        .withCredentials(credentials)
                        .build(), address);
    }
}
