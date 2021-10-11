package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class ClientFactoryIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String PASSWORD;

    @BeforeAll
    public static void setUp() throws TimeoutException {
        startCluster();

        WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer);
        waitingConsumer.waitUntil(f -> f.getUtf8String().contains("The cluster is balanced ok"));

        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    @Test
    void test_should_createProxyRetryingClientWithMappedCrudMethodName() {
        //given
        int expectedNumberOfAttempts = 5;
        int expectedDelay = 500;
        int expectedRequestTimeout = 123;
        String expectedSelectFunctionName = "custom_crud_select";
        Function<Throwable, Boolean> expectedCallback =
                throwable -> throwable.getMessage().equals("Hello World");

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
        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
        assertDoesNotThrow(() -> client.space("test_space").select(Conditions.any()).join());
    }
}
