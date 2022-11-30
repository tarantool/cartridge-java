package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.core.RetryingTarantoolTupleClient;
import io.tarantool.driver.exceptions.TarantoolAttemptsLimitException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolFunctionCallException;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.exceptions.TarantoolTimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static java.lang.Math.abs;
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
            assertTrue(e.getCause().getCause() instanceof TarantoolInternalException);
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
                .unbound(e -> e.getMessage().contains("Unsuccessful attempt"))
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
                .unbound(t -> true)
                .withRequestTimeout(20) //requestTimeout
                .withOperationTimeout(1000)  //operationTimeout
                .build());

        try {
            retryingClient.call("raising_error").get();
        } catch (ExecutionException e) {
            assertEquals(ExecutionException.class, e.getClass());
            assertEquals(TarantoolTimeoutException.class, e.getCause().getClass());
            assertTrue(e.getCause().getMessage().contains("Operation timeout value exceeded after"));
            assertEquals(TarantoolInternalException.class, e.getCause().getCause().getClass());
            assertTrue(e.getCause().getCause().getMessage().contains(
                "InnerErrorMessage:\n" +
                    "code: 32\n" +
                    "message:"));
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

    void longTermFunction(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        ArrayList<CompletableFuture<List<?>>> list = new ArrayList<>(1000);
        int batchSize = 1000;

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < batchSize; j++) {
                CompletableFuture<List<?>> future = client
                    .call("simple_long_running_function", Collections.singletonList(0.01));
                list.add(j, future);
            }
            for (int j = 0; j < batchSize; j++) {
                assertEquals(list.get(j).join().get(0), true);
            }
            list.clear();
        }
    }

    @Test
    void testAsyncRetryingPerformance() throws InterruptedException, ExecutionException, TimeoutException {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> asyncClient
            = TarantoolClientFactory.createClient()
            .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
            .withAddress(container.getRouterHost(), container.getRouterPort())
            .withConnectTimeout(1000)
            .withReadTimeout(1000)
            .withRetryingIndefinitely(policy -> policy.withDelay(500)
                .withRequestTimeout(2000)
                .withOperationTimeout(2000))
            .build();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client
            = TarantoolClientFactory.createClient()
            .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
            .withAddress(container.getRouterHost(), container.getRouterPort())
            .withConnectTimeout(1000)
            .withReadTimeout(1000)
            .build();

        CompletableFuture<Boolean> syncFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> asyncFuture = new CompletableFuture<>();

        AtomicReference<Timestamp> asyncTimestamp = new AtomicReference<>();
        AtomicReference<Timestamp> syncTimestamp = new AtomicReference<>();

        CompletableFuture<Void> asyncFutureTimestamp =
            asyncFuture.thenRun(() -> asyncTimestamp.set(new Timestamp(System.currentTimeMillis())));
        CompletableFuture<Void> syncFutureTimestamp =
            syncFuture.thenRun(() -> syncTimestamp.set(new Timestamp(System.currentTimeMillis())));

        new Thread(() -> {
            longTermFunction(client);
            syncFuture.complete(true);
        }).start();
        new Thread(() -> {
            longTermFunction(asyncClient);
            asyncFuture.complete(true);
        }).start();

        CompletableFuture.allOf(asyncFutureTimestamp, syncFutureTimestamp).get(30, TimeUnit.SECONDS);
        assertTrue(abs(asyncTimestamp.get().getTime() - asyncTimestamp.get().getTime()) < 500);
    }

    @Test
    public void test_incorrectCallback_shouldHandleCorrectly() {
        //given
        Predicate<Throwable> callbackWithException = e -> {
            // it's possible, when user will specify incorrect callback
            // such as throwable.getMessage().equals() and message is null
            throw new NullPointerException();
        };

        //when
        List<TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>> clients = Arrays.asList(
            TarantoolClientFactory.createClient()
                .withAddress(container.getRouterHost(), container.getRouterPort())
                .withCredentials(USER_NAME, PASSWORD)
                .withRetryingByNumberOfAttempts(5, callbackWithException, policy -> policy)
                .build(),
            TarantoolClientFactory.createClient()
                .withAddress(container.getRouterHost(), container.getRouterPort())
                .withCredentials(USER_NAME, PASSWORD)
                .withRetryingIndefinitely(callbackWithException, policy -> policy)
                .build()
        );

        for (TarantoolClient client :
            clients) {
            //then
            CompletionException completionException =
                assertThrows(CompletionException.class, () -> client.eval("return error").join());
            assertTrue(completionException.getCause() instanceof TarantoolClientException);
            assertTrue(completionException.getMessage()
                .contains("Specified in TarantoolClient predicate for exception check threw exception: "));
        }
    }
}
