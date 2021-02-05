package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Alexey Kuzin
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
    void testInfiniteRetryPolicy_unretryableError() throws ExecutionException, InterruptedException {
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
    void testAttemptsBoundRetryPolicy_returnSuccessAfterFails() throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = new TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy<>(4, throwable -> true);
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> {
                    retries.set(retries.get() - 1);
                    return failingIfAvailableRetriesFuture(retries.get());
                }, executor);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testAttemptsBoundRetryPolicy_runOutOfAttempts() throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = new TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy<>(3, throwable -> true);
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> {
                    retries.set(retries.get() - 1);
                    return failingIfAvailableRetriesFuture(retries.get());
                }, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            assertEquals(RuntimeException.class, e.getCause().getClass());
            assertEquals("Should fail " + retries.get() + " times", e.getCause().getMessage());
        }
    }

    @Test
    void testAttemptsBoundRetryPolicy_zeroAttempts() throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(1);
        RequestRetryPolicy policy = new TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy<>(0, throwable -> true);
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> {
                    retries.set(retries.get() - 1);
                    return failingIfAvailableRetriesFuture(retries.get());
                }, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            assertEquals(RuntimeException.class, e.getCause().getClass());
            assertEquals("Should fail " + retries.get() + " times", e.getCause().getMessage());
        }
    }

    @Test
    void testAttemptsBoundRetryPolicy_unretryableError() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = new TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy<>(4, throwable -> true);
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::simpleFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Fail", e.getCause().getMessage());
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

    private CompletableFuture<Boolean> failingIfAvailableRetriesFuture(int retries) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (retries > 0) {
            result.completeExceptionally(new RuntimeException("Should fail " + retries + " times"));
        } else {
            result.complete(true);
        }
        return result;
    }
}
