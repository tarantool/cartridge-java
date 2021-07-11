package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.exceptions.TarantoolAttemptsLimitException;
import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.exceptions.TarantoolServerException;
import io.tarantool.driver.exceptions.TarantoolServerInternalException;
import io.tarantool.driver.exceptions.TarantoolTimeoutException;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
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
 * @author Artyom Dubinin
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
    void testAttemptsBoundSuccess() throws Exception {
        try (ProxyTarantoolTupleClient client = setupClient()) {
            // The stored function will fail 3 times
            client.call("setup_retrying_function", Collections.singletonList(3));

            String result = retrying(client, 3).callForSingleResult("retrying_function", String.class).get();
            assertEquals("Success", result);
        }
    }

    @Test
    void testAttemptsBoundLimitReached_withTarantoolException() {
        try {
            ProxyTarantoolTupleClient client = setupClient();
            client.call("setup_retrying_function", Collections.singletonList(3));
            retrying(client, 2).callForSingleResult("retrying_function", String.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            assertTrue(e.getCause() instanceof TarantoolAttemptsLimitException);
            assertTrue(e.getCause().getMessage().contains("Attempts limit reached:"));
            assertTrue(e.getCause().getCause() instanceof TarantoolServerInternalException);
        }
    }

    @Test
    void testAttemptsBoundLimitReached_withTimeout() throws Exception {
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
            assertTrue(e.getCause() instanceof TarantoolAttemptsLimitException, "Unexpected exception type: " + e);
            assertTrue(e.getCause().getCause() instanceof TimeoutException, "Unexpected exception type: " + e);
        }
    }

    @Test
    void testInfiniteRetrySuccess() throws Exception {
        ProxyTarantoolTupleClient client = setupClient();

        RetryingTarantoolTupleClient retryingClient = new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies
                        .unbound(e -> {
                            return e.getMessage().contains("Unsuccessful attempt");
                        })
                        .withRequestTimeout(200) //requestTimeout
                        .withOperationTimeout(2000)  //operationTimeout
                        .build());

        client.call("setup_retrying_function", Collections.singletonList(3));
        CompletableFuture<String> f = retryingClient
                .callForSingleResult("retrying_function", String.class);

        assertEquals("Success", f.get());
    }

    /**
     * This test covers https://github.com/tarantool/cartridge-java/issues/83
     */
    @Test
    void testInfiniteRetryContinued_afterGetFailed() throws Exception {
        ProxyTarantoolTupleClient client = setupClient();

        client.call("reset_request_counters");

        AtomicLong counter = new AtomicLong(0);
        RetryingTarantoolTupleClient retryingClient = new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies
                        .unbound(e -> {
                            counter.incrementAndGet();
                            return !(e instanceof TarantoolFunctionCallException);
                        })
                        .withRequestTimeout(10) //requestTimeout
                        .withOperationTimeout(200)  //operationTimeout
                        .build());

        CompletableFuture<List<?>> f = retryingClient
                .call("long_running_function", Collections.singletonList(10));

        assertThrows(TimeoutException.class, () -> f.get(100, TimeUnit.MILLISECONDS));
        assertFalse(f.isDone());

        // check that request was executed at least 2 times
        long counterVal = counter.get();
        assertTrue(counterVal > 1, "Request was not executed by policy.");

        // check that worker continues to execute requests until operationTimeout fires
        Thread.sleep(300);
        assertTrue(counter.get() > counterVal, "Future completed too early");

        //check that all worker threads has stopped (counter remain unchanged)
        counterVal = counter.get();
        Thread.sleep(100);
        assertEquals(counterVal, counter.get(), "Policy continues executing request after timeout");
    }

    /**
     * This test covers https://github.com/tarantool/cartridge-java/issues/82
     */
    @Test
    void testInfiniteRetryTimeoutReached_withTarantoolException() throws Exception {
        ProxyTarantoolTupleClient client = setupClient();

        RetryingTarantoolTupleClient retryingClient = new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies
                        .unbound()
                        .withRequestTimeout(20) //requestTimeout
                        .withOperationTimeout(200)  //operationTimeout
                        .build());

        CompletableFuture<List<?>> f = retryingClient
                .call("raising_error", Collections.singletonList(10));

        try {
            f.get();
        } catch (ExecutionException e) {
            assertEquals(ExecutionException.class, e.getClass());
            assertEquals(TarantoolTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("Operation timeout value exceeded after"));
            assertEquals(TarantoolServerException.class, e.getCause().getCause().getClass());
            assertTrue(e.getCause().getCause().getMessage().contains("Test error: raising_error() called"));
        }
    }

    @Test
    void testInfiniteRetryTimeoutReached_withTimeout() throws Exception {
        ProxyTarantoolTupleClient client = setupClient();

        RetryingTarantoolTupleClient retryingClient = new RetryingTarantoolTupleClient(client,
                TarantoolRequestRetryPolicies
                        .unbound()
                        .withRequestTimeout(20) //requestTimeout
                        .withOperationTimeout(200)  //operationTimeout
                        .build());

        client.call("reset_request_counters");
        CompletableFuture<List<?>> f = retryingClient
                .call("long_running_function", Collections.singletonList(10));

        try {
            f.get();
        } catch (ExecutionException e) {
            assertEquals(ExecutionException.class, e.getClass());
            assertEquals(TarantoolTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("Operation timeout value exceeded after"));
            assertEquals(TimeoutException.class, e.getCause().getCause().getClass());
        }
    }
}
