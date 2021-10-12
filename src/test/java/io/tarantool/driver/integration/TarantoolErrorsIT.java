package io.tarantool.driver.integration;

import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.exceptions.TarantoolInternalNetworkException;
import io.tarantool.driver.core.RetryingTarantoolTupleClient;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TarantoolErrorsIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    @BeforeAll
    public static void setUp() {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    private ProxyTarantoolTupleClient setupClient() {
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000)
                .withReadTimeout(1000)
                .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
                config, container.getRouterHost(), container.getRouterPort());
        return new ProxyTarantoolTupleClient(clusterClient);
    }

    private RetryingTarantoolTupleClient setupRetryingClient(int retries) {
        ProxyTarantoolTupleClient client = setupClient();
        return new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies
                        .byNumberOfAttempts(retries).build());
    }

    @Test
    void testNetworkError_boxErrorUnpackNoConnection() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForSingleResult("box_error_unpack_no_connection", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 77"));
            assertTrue(message.contains("message: Connection is not established"));
            assertTrue(message.contains("type: ClientError"));
            assertTrue(message.contains("trace:"));
        }
    }

    @Test
    void testNetworkError_boxErrorUnpackTimeout() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForMultiResult("box_error_unpack_timeout",
                    Collections.singletonList("Some error"),
                    ArrayList::new,
                    String.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 78"));
            assertTrue(message.contains("message: Timeout exceeded"));
            assertTrue(message.contains("type: ClientError"));
            assertTrue(message.contains("trace:"));
        }
    }

    @Test
    void testNetworkError_boxErrorTimeout() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForMultiResult("box_error_timeout",
                    Collections.singletonList("Some error"),
                    ArrayList::new,
                    String.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 78"));
            assertTrue(message.contains("message: Timeout exceeded"));
        }
    }

    @Test
    void testNetworkError_crudErrorTimeout() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForSingleResult("crud_error_timeout", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("\"code\":78"));
            assertTrue(message.contains("\"type\":\"ClientError\""));
            assertTrue(message.contains("\"message\":\"Timeout exceeded\""));
            assertTrue(message.contains("\"trace\":"));
        }
    }

    @Test
    void testNonNetworkError_boxErrorUnpack() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForSingleResult("box_error_non_network_error", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalException);
            assertFalse(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 40"));
            assertTrue(message.contains("message: Failed to write to disk"));
            assertTrue(message.contains("type: ClientError"));
            assertTrue(message.contains("trace:"));
        }
    }

    @Test
    void testNonNetworkError_boxError() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForSingleResult("raising_error", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalException);
            assertFalse(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("InnerErrorMessage:"));
            assertTrue(message.contains("code: 32"));
            assertTrue(message.contains("message:"));
        }
    }
}
