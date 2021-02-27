package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.RetryingTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolRequestRetryPolicies;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Alexey Kuzin
 */
public class RetryingTarantoolTupleClientIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    private ProxyTarantoolTupleClient setupClient() {
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(100000)
                .withReadTimeout(100000)
                .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
                config, container.getRouterHost(), container.getRouterPort());
        return new ProxyTarantoolTupleClient(clusterClient);
    }

    private RetryingTarantoolTupleClient retrying(ProxyTarantoolTupleClient client, int retries) {
        return new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies.byNumberOfAttempts(
                    retries, e -> e.getMessage().contains("Unsuccessful attempt")
                ).build());
    }

    @Test
    void testSuccessAfterSeveralRetries() throws Exception {
        try (ProxyTarantoolTupleClient client = setupClient()) {
            // The stored function will fail 3 times
            client.call("setup_retrying_function", Collections.singletonList(3));

            String result = retrying(client, 4).callForSingleResult("retrying_function", String.class).get();
            assertEquals("Success", result);
        }
    }
}
