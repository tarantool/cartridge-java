package io.tarantool.driver.api.retry;

import io.tarantool.driver.exceptions.TarantoolAttemptsLimitException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolConnectionException;
import io.tarantool.driver.exceptions.TarantoolInternalNetworkException;
import io.tarantool.driver.exceptions.TarantoolTimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("Fail", ex.getCause().getMessage());
    }

    @Test
    void testInfiniteRetryPolicy_shouldHandleCorrectly_ifOperationThrowException() throws InterruptedException {
        RequestRetryPolicy policy = throwable -> throwable instanceof TarantoolClientException;
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::operationWithException, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("Fail", ex.getCause().getMessage());
    }

    @Test
    void testInfiniteRetryPolicyGetters_shouldWorkCorrectly() {
        RequestRetryPolicy policy = throwable -> true;
        assertEquals(500, policy.getDelay());
        assertEquals(TimeUnit.HOURS.toMillis(1), policy.getRequestTimeout());
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
    void testUnboundRetryPolicyFactoryGetCallback_shouldWorkCorrectly() {
        Predicate<Throwable> expectedCallback = throwable -> true;
        TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory<Predicate<Throwable>> policyFactory =
            TarantoolRequestRetryPolicies.unbound(expectedCallback).build();
        assertEquals(expectedCallback, policyFactory.getCallback());
    }

    @Test
    void testUnboundRetryPolicy_shouldHandleCorrectly_ifOperationThrowException() throws InterruptedException {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.unbound().withDelay(10).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::operationWithException, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("Fail", ex.getCause().getMessage());
    }

    @Test
    void testUnboundRetryPolicyGetOperationTimeout_shouldWorkCorrectly() throws InterruptedException {
        long expectedOperationTimeout = 123;
        TarantoolRequestRetryPolicies.InfiniteRetryPolicy<Predicate<Throwable>> policy =
            new TarantoolRequestRetryPolicies.InfiniteRetryPolicy<>(
                0, expectedOperationTimeout, 0, throwable -> true);
        assertEquals(expectedOperationTimeout, policy.getOperationTimeout());
    }

    @Test
    void testUnboundRetryPolicyCanRetryRequest_shouldWorkCorrectly() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(2);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.unbound(
            ex -> counter.getAndDecrement() > 0
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::simpleFailingFuture, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertEquals(-1, counter.get());
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("Fail", ex.getCause().getMessage());
    }

    @Test
    void testUnboundRetryPolicy_shouldWorkCorrectly_ifFutureNeverCompletes() throws InterruptedException {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.unbound()
            .withOperationTimeout(500)
            .withRequestTimeout(500)
            .build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::neverCompletedFuture, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof TarantoolTimeoutException);
        assertEquals("Operation timeout value exceeded after 500 ms", ex.getCause().getMessage());
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
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertEquals(TarantoolAttemptsLimitException.class, ex.getCause().getClass());
        assertEquals("Attempts limit reached: 3", ex.getCause().getMessage());
        assertEquals(TarantoolInternalNetworkException.class, ex.getCause().getCause().getClass());
        assertEquals("Should fail 1 times", ex.getCause().getCause().getMessage());
    }

    @Test
    void testAttemptsBoundRetryPolicy_zeroAttempts() throws InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(1);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(0).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        ExecutionException thrown = null;
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertEquals(TarantoolAttemptsLimitException.class, ex.getCause().getClass());
        assertEquals("Attempts limit reached: 0", ex.getCause().getMessage());
        assertEquals(RuntimeException.class, ex.getCause().getCause().getClass());
        assertEquals("Should fail 1 times", ex.getCause().getCause().getMessage());
    }

    @Test
    void testAttemptsBoundRetryPolicy_unretryableError() throws InterruptedException {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy
            .wrapOperation(this::simpleNetworkFailingFuture, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof TarantoolAttemptsLimitException);
        assertEquals("Attempts limit reached: 4", ex.getCause().getMessage());
        assertTrue(ex.getCause().getCause() instanceof RuntimeException);
        assertEquals("Fail", ex.getCause().getCause().getMessage());
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
    void testAttemptsBoundRetryPolicy_shouldHandleCorrectly_ifOperationThrowException() throws InterruptedException {
        RequestRetryPolicy policy = new TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy<>(
            4, 10, 0, throwable -> throwable instanceof TimeoutException);
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::operationWithException, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("Fail", ex.getCause().getMessage());
    }

    @Test
    void testWithNetworkErrors_retryWhileTimeoutError() {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture =
            policy.wrapOperation(this::timeoutExceptionFailingFuture, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof TarantoolAttemptsLimitException);
        assertEquals("Attempts limit reached: 4", ex.getCause().getMessage());
        assertTrue(ex.getCause().getCause() instanceof TimeoutException);
        assertEquals("Fail", ex.getCause().getCause().getMessage());
    }

    @Test
    void testWithNetworkErrors_retryWhileTarantoolConnectionError() {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            this::tarantoolConnectionExceptionFailingFuture, executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof TarantoolAttemptsLimitException);
        assertEquals("Attempts limit reached: 4", ex.getCause().getMessage());
        assertTrue(ex.getCause().getCause() instanceof TarantoolConnectionException);
        assertEquals("The client is not connected to Tarantool server",
            ex.getCause().getCause().getMessage());
    }

    @Test
    void testWithNetworkErrors_retryWhileTarantoolServerInternalNetworkError() {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            this::tarantoolServerInternalNetworkExceptionFailingFuture, executor);

        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof TarantoolAttemptsLimitException);
        assertEquals("Attempts limit reached: 4", ex.getCause().getMessage());
        assertTrue(ex.getCause().getCause() instanceof TarantoolInternalNetworkException);
        assertEquals("code: 77",
            ex.getCause().getCause().getMessage());
    }

    @Test
    void testWithNetworkErrors_notNetworkError() {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3,
            TarantoolRequestRetryPolicies.retryNetworkErrors()
        ).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
            () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        ExecutionException ex = assertThrows(ExecutionException.class, wrappedFuture::get);
        assertTrue(ex.getCause() instanceof RuntimeException);
        assertEquals("Should fail 3 times", ex.getCause().getMessage());
        assertEquals(2, retries.get());
    }

    private CompletableFuture<Boolean> simpleSuccessFuture() {
        return CompletableFuture.completedFuture(true);
    }

    private CompletableFuture<Boolean> simpleFailingFuture() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Fail"));
        return result;
    }

    private CompletableFuture<Boolean> neverCompletedFuture() {
        return new CompletableFuture<>();
    }

    private CompletableFuture<Boolean> operationWithException() {
        throw new RuntimeException("Fail");
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
