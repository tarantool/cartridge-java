package io.tarantool.driver.retry;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.client.TarantoolClientFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.tarantool.driver.api.client.TarantoolConnectionSelectionStrategyType.ROUND_ROBIN;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RetryTarantoolClientBuilderTest {

    private final TarantoolServerAddress SAMPLE_ADDRESS =
            new TarantoolServerAddress("123.123.123.123", 32323);

    private final TarantoolCredentials SAMPLE_CREDENTIALS =
            new SimpleTarantoolCredentials("root", "passwd");

    private final DefaultMessagePackMapper SAMPLE_MAPPER =
            new DefaultMessagePackMapper.Builder().build();

    @Test
    void test_should_createProxyRetryingClient() {
        //given
        int expectedNumberOfAttempts = 5;
        int expectedDelay = 500;
        int expectedRequestTimeout = 123;
        String expectedReplaceFunctionName = "hello";
        String expectedTruncateFunctionName = "create";
        Function<Throwable, Boolean> expectedCallback =
                throwable -> throwable.getMessage().equals("Hello World");

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createClient()
                        .withAddress(SAMPLE_ADDRESS)
                        .withCredentials(SAMPLE_CREDENTIALS)
                        .withMessagePackMapper(SAMPLE_MAPPER)
                        .withRequestRetryAttempts(expectedNumberOfAttempts)
                        .withRequestRetryDelay(expectedDelay)
                        .withConnectionSelectionStrategy(ROUND_ROBIN)
                        .withProxyMethodMapping(builder -> builder
                                .withReplaceFunctionName(expectedReplaceFunctionName)
                                .withTruncateFunctionName(expectedTruncateFunctionName)
                        )
                        .withRequestRetryExceptionCallback(expectedCallback)
                        .withRequestRetryTimeout(expectedRequestTimeout)
                        .build();

        //then
        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());

        // assert base params
        TarantoolClientConfig config = client.getConfig();
        assertEquals(1, config.getConnections());
        assertEquals(1000, config.getReadTimeout());
        assertEquals(1000, config.getConnectTimeout());
        assertEquals(2000, config.getRequestTimeout());
        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());

        // assert retry params
        RetryingTarantoolTupleClient retryingTarantoolTupleClient = (RetryingTarantoolTupleClient) client;
        ProxyOperationsMappingConfig actualMappingConfig = ((ProxyTarantoolTupleClient)
                (retryingTarantoolTupleClient).getClient()).getMappingConfig();

        TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?> retryPolicyFactory =
                (TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?>)
                        retryingTarantoolTupleClient.getRetryPolicyFactory();

        assertEquals(expectedCallback, retryPolicyFactory.getExceptionCheck());
        assertEquals(expectedDelay, retryPolicyFactory.getDelay());
        assertEquals(expectedNumberOfAttempts, retryPolicyFactory.getNumberOfAttempts());
        assertEquals(expectedRequestTimeout, retryPolicyFactory.getRequestTimeout());


        // assert proxy params
        assertEquals(expectedReplaceFunctionName, actualMappingConfig.getReplaceFunctionName());
        assertEquals(expectedTruncateFunctionName, actualMappingConfig.getTruncateFunctionName());
    }
}
