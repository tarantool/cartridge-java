package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
                .withConnectTimeout(1000)
                .withReadTimeout(1000)
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

            String result = retrying(client, 3).callForSingleResult("retrying_function", String.class).get();
            assertEquals("Success", result);
        }
    }

    @Test
    void testFailAfterSeveralRetries() throws Exception {
        try {
            ProxyTarantoolTupleClient client = setupClient();
            client.call("setup_retrying_function", Collections.singletonList(3));
            retrying(client, 2).callForSingleResult("retrying_function", String.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof TarantoolFunctionCallException);
        }
    }

    @Test
    void testNumberOfAttemptsPolicyFailAfterTimeout() throws Exception {
        ProxyTarantoolTupleClient client = setupClient();

        client.call("reset_request_counters");

        RetryingTarantoolTupleClient retryingClient = new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies
                        .byNumberOfAttempts(0)
                        .withRequestTimeout(1000)
                        .build());

        try {
            retryingClient.call("long_running_function", Collections.singletonList(10)).get();
            fail("Exception must be thrown after timeout.");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException, "Unexpected exception type: " + e);
        }
    }

    /**
     * This test covers https://github.com/tarantool/cartridge-java/issues/83
     */
    @Test
    void testUnboundPolicyFailsAfterTimeout() throws Exception {
        ProxyTarantoolTupleClient client = setupClient();

        client.call("reset_request_counters");

        final int TOTAL_TIMEOUT = 1000;
        final int PER_REQUEST_TIMEOUT = 100;

        assertTrue(TOTAL_TIMEOUT > PER_REQUEST_TIMEOUT * 3);

        AtomicLong counter = new AtomicLong(0);
        RetryingTarantoolTupleClient retryingClient = new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies
                        .unbound(e -> {
                            counter.incrementAndGet();
                            return !(e instanceof TarantoolFunctionCallException);
                        })
                        .withRequestTimeout(PER_REQUEST_TIMEOUT) //requestTimeout
                        .withOperationTimeout(TOTAL_TIMEOUT)  //operationTimeout
                        .build());

        CompletableFuture<List<?>> f = retryingClient
                .call("long_running_function", Collections.singletonList(20));

        assertThrows(TimeoutException.class, () -> f.get(500, TimeUnit.MILLISECONDS));
        assertFalse(f.isDone());

        long counterAfterCompletion = counter.get();
        assertTrue(counterAfterCompletion > 1,
                "Request was not executed by policy.");
        Thread.sleep(1500);
        //check that all worker threads has stopped (counter remain unchanged)
        assertTrue(counterAfterCompletion < counter.get(),
                "Future completed too early");
        counterAfterCompletion = counter.get();
        Thread.sleep(500);
        assertEquals(counterAfterCompletion, counter.get(),
                "Policy continues executing request after timeout");
    }
}
