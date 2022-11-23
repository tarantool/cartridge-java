package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.RetryingTarantoolTupleClient;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ClientFactoryIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    @BeforeAll
    public static void setUp() throws TimeoutException {
        startCluster();

        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    @Test
    public void test_should_createProxyRetryingClientWithMappedCrudMethodName() {
        //given
        int expectedNumberOfAttempts = 5;
        int expectedDelay = 500;
        int expectedRequestTimeout = 123;
        String expectedSelectFunctionName = "custom_crud_select";
        Predicate<Throwable> expectedCallback = e -> {
            // it's possible, when user will specify incorrect callback
            // such as throwable.getMessage().equals() and message is null
            throw new NullPointerException();
        };

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
            TarantoolClientFactory.createClient()
                .withAddress(container.getRouterHost(), container.getRouterPort())
                .withCredentials(USER_NAME, PASSWORD)
                .withProxyMethodMapping(builder -> builder
                    .withSelectFunctionName(expectedSelectFunctionName)
                )
                .withRetryingByNumberOfAttempts(expectedNumberOfAttempts, expectedCallback,
                    policy -> policy.withDelay(expectedDelay)
                        .withRequestTimeout(expectedRequestTimeout))
                .build();
        //then
        CompletionException completionException =
            assertThrows(CompletionException.class, () -> client.eval("return error").join());
        assertTrue(completionException.getCause() instanceof TarantoolClientException);
        assertTrue(completionException.getMessage()
            .contains("Specified in TarantoolClient predicate for exception check threw exception: "));

        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
        assertDoesNotThrow(() -> client.space("test_space").select(Conditions.any()).join());
    }
}
