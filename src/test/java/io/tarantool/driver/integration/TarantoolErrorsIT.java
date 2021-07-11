package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.exceptions.TarantoolServerInternalException;
import io.tarantool.driver.exceptions.TarantoolServerInternalNetworkException;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
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
    void testNetworkError_noConnection() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForSingleResult("box_error_unpack_no_connection", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof TarantoolServerInternalNetworkException);
            assertTrue(e.getCause().getMessage().contains(
                "code: 77\n" +
                "message: Connection is not established\n" +
                "base_type: ClientError\n" +
                "type: ClientError\n" +
                "trace:")
            );
        }
    }

    @Test
    void testNetworkError_timeout() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForMultiResult("box_error_unpack_timeout",
                    Collections.singletonList("Some error"),
                    ArrayList::new,
                    String.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof TarantoolServerInternalNetworkException);
            assertTrue(e.getCause().getMessage().contains(
                "code: 78\n" +
                "message: Timeout exceeded\n" +
                "base_type: ClientError\n" +
                "type: ClientError\n" +
                "trace:")
            );
        }
    }

    @Test
    void testNetworkError_crudErrorTimeout() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForSingleResult("crud_error_timeout", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof TarantoolServerInternalNetworkException);
            assertTrue(e.getCause().getMessage().contains(
                    "Function returned an error: {\"code\":78," +
                            "\"base_type\":\"ClientError\"," +
                            "\"type\":\"ClientError\"," +
                            "\"message\":\"Timeout exceeded\","
            ));
        }
    }

    @Test
    void testNonNetworkError() {
        try {
            ProxyTarantoolTupleClient client = setupClient();

            client.callForSingleResult("box_error_non_network_error", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof TarantoolServerInternalException);
            assertFalse(e.getCause() instanceof TarantoolServerInternalNetworkException);
            assertTrue(e.getCause().getMessage().contains(
                "code: 40\n" +
                "message: Failed to write to disk\n" +
                "base_type: ClientError\n" +
                "type: ClientError\n" +
                "trace:")
            );
        }
    }
}
