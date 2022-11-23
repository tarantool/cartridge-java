package io.tarantool.driver.api.retry;

import io.tarantool.driver.exceptions.TarantoolAttemptsLimitException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolConnectionException;
import io.tarantool.driver.exceptions.TarantoolInternalNetworkException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class RequestRetryPolicyTest {

    private final Executor executor = Executors.newWorkStealingPool();

    @Test
    void testInfiniteRetryPolicy_success() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = throwable -> true;
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::simpleSuccessFuture, executor);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testInfiniteRetryPolicy_successWithFuture() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = throwable -> true;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(() -> future, executor);
        future.complete(true);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testInfiniteRetryPolicy_successAfterFails() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = throwable -> true;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(() -> future, executor);
        future.complete(true);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testInfiniteRetryPolicy_unretryableError() throws InterruptedException {
        RequestRetryPolicy policy = throwable -> throwable instanceof TarantoolClientException;
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::simpleFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Fail", e.getCause().getMessage());
        }
    }

    @Test
    void testUnboundRetryPolicy_returnSuccessAfterFailsWithDelay() throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.unbound().withDelay(10).build().create();
        Instant now = Instant.now();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingWithNetworkIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        assertTrue(wrappedFuture.get());
        long diff = Instant.now().toEpochMilli() - now.toEpochMilli();
        assertTrue(diff >= 30);
    }

    @Test
    void testAttemptsBoundRetryPolicy_returnSuccessAfterFails() throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingWithNetworkIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testAttemptsBoundRetryPolicy_returnSuccessAfterFailsWithDelay()
        throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3).withDelay(10).build().create();
        Instant now = Instant.now();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingWithNetworkIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        assertTrue(wrappedFuture.get());
        long diff = Instant.now().toEpochMilli() - now.toEpochMilli();
        assertTrue(diff >= 30);
    }

    @Test
    void testAttemptsBoundRetryPolicy_runOutOfAttempts() throws InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(4);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingWithNetworkIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        ExecutionException thrown = null;
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            thrown = e;
            assertEquals(TarantoolAttemptsLimitException.class, e.getCause().getClass());
            assertEquals("Attempts limit reached: 3", e.getCause().getMessage());
            assertEquals(TarantoolInternalNetworkException.class, e.getCause().getCause().getClass());
            assertEquals("Should fail 1 times", e.getCause().getCause().getMessage());
        }
        assertNotNull(thrown, "No exception has been thrown");
    }

    @Test
    void testAttemptsBoundRetryPolicy_zeroAttempts() throws InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(1);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(0).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        ExecutionException thrown = null;
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            thrown = e;
            assertEquals(TarantoolAttemptsLimitException.class, e.getCause().getClass());
            assertEquals("Attempts limit reached: 0", e.getCause().getMessage());
            assertEquals(RuntimeException.class, e.getCause().getCause().getClass());
            assertEquals("Should fail 1 times", e.getCause().getCause().getMessage());
        }
        assertNotNull(thrown, "No exception has been thrown");
    }

    @Test
    void testAttemptsBoundRetryPolicy_unretryableError() throws InterruptedException {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy
            .wrapOperation(this::simpleNetworkFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TarantoolAttemptsLimitException);
            assertEquals("Attempts limit reached: 4", e.getCause().getMessage());
            assertTrue(e.getCause().getCause() instanceof RuntimeException);
            assertEquals("Fail", e.getCause().getCause().getMessage());
        }
    }

    @Test
    void testAttemptsBoundRetryPolicy_retryTimeouts() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = new TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy<>(
            4, 10, 0, throwable -> throwable instanceof TimeoutException);
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> sleepIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1), 20), executor);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testWithNetworkErrors_retryWhileTimeoutError() {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture =
            policy.wrapOperation(this::timeoutExceptionFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            assertTrue(e.getCause() instanceof TarantoolAttemptsLimitException);
            assertEquals("Attempts limit reached: 4", e.getCause().getMessage());
            assertTrue(e.getCause().getCause() instanceof TimeoutException);
            assertEquals("Fail", e.getCause().getCause().getMessage());
        }
    }

    @Test
    void testWithNetworkErrors_retryWhileTarantoolConnectionError() {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            this::tarantoolConnectionExceptionFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            assertTrue(e.getCause() instanceof TarantoolAttemptsLimitException);
            assertEquals("Attempts limit reached: 4", e.getCause().getMessage());
            assertTrue(e.getCause().getCause() instanceof TarantoolConnectionException);
            assertEquals("The client is not connected to Tarantool server",
                e.getCause().getCause().getMessage());
        }
    }

    @Test
    void testWithNetworkErrors_retryWhileTarantoolServerInternalNetworkError() {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            this::tarantoolServerInternalNetworkExceptionFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            assertTrue(e.getCause() instanceof TarantoolAttemptsLimitException);
            assertEquals("Attempts limit reached: 4", e.getCause().getMessage());
            assertTrue(e.getCause().getCause() instanceof TarantoolInternalNetworkException);
            assertEquals("code: 77",
                e.getCause().getCause().getMessage());
        }
    }

    @Test
    void testWithNetworkErrors_notNetworkError() {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Should fail 3 times", e.getCause().getMessage());
            assertEquals(2, retries.get());
        }
    }

    private CompletableFuture<Boolean> simpleSuccessFuture() {
        return CompletableFuture.completedFuture(true);
    }

    private CompletableFuture<Boolean> simpleFailingFuture() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Fail"));
        return result;
    }

    private CompletableFuture<Boolean> simpleNetworkFailingFuture() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new TarantoolInternalNetworkException("Fail"));
        return result;
    }

    private CompletableFuture<Boolean> timeoutExceptionFailingFuture() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new TimeoutException("Fail"));
        return result;
    }

    private CompletableFuture<Boolean> tarantoolConnectionExceptionFailingFuture() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new TarantoolConnectionException(new RuntimeException()));
        return result;
    }

    private CompletableFuture<Boolean> tarantoolServerInternalNetworkExceptionFailingFuture() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new TarantoolInternalNetworkException("code: 77"));
        return result;
    }

    private CompletableFuture<Boolean> failingIfAvailableRetriesFuture(int retries) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (retries > 0) {
            result.completeExceptionally(new RuntimeException("Should fail " + retries + " times"));
        } else {
            result.complete(true);
        }
        return result;
    }

    private CompletableFuture<Boolean> failingWithNetworkIfAvailableRetriesFuture(int retries) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (retries > 0) {
            result.completeExceptionally(new TarantoolInternalNetworkException("Should fail " + retries + " times"));
        } else {
            result.complete(true);
        }
        return result;
    }

    private CompletableFuture<Boolean> sleepIfAvailableRetriesFuture(int retries, long timeout) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (retries > 0) {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        result.complete(true);
        return result;
    }
}
