package io.tarantool.driver.api;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.tarantool.driver.api.TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN;
import static io.tarantool.driver.api.TarantoolConnectionSelectionStrategyType.ROUND_ROBIN;
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
                        .withAddresses(SAMPLE_ADDRESS)
                        .withCredentials(SAMPLE_CREDENTIALS)
                        .withMessagePackMapper(SAMPLE_MAPPER)
                        .withConnectionSelectionStrategy(ROUND_ROBIN)
                        .withProxyMethodMapping(builder -> builder
                                .withReplaceFunctionName(expectedReplaceFunctionName)
                                .withTruncateFunctionName(expectedTruncateFunctionName)
                        )
                        .withRetryingByNumberOfAttempts(expectedNumberOfAttempts, expectedCallback,
                                policy -> policy.withRequestTimeout(expectedRequestTimeout)
                                        .withDelay(expectedDelay))
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

        TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?> retryPolicyFactory =
                (TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?>)
                        retryingTarantoolTupleClient.getRetryPolicyFactory();

        assertEquals(expectedCallback, retryPolicyFactory.getExceptionCheck());
        assertEquals(expectedDelay, retryPolicyFactory.getDelay());
        assertEquals(expectedNumberOfAttempts, retryPolicyFactory.getNumberOfAttempts());
        assertEquals(expectedRequestTimeout, retryPolicyFactory.getRequestTimeout());
    }

    @Test
    void test_should_createRetryingClient() {
        //given
        int expectedNumberOfAttempts = 5;

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddresses(SAMPLE_ADDRESS)
                .withCredentials(SAMPLE_CREDENTIALS)
                .withMessagePackMapper(SAMPLE_MAPPER)
                .withRetryingByNumberOfAttempts(expectedNumberOfAttempts)
                .build();

        int actualNumberOfAttempts = ((TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?>)
                ((RetryingTarantoolTupleClient) client).getRetryPolicyFactory()).getNumberOfAttempts();

        //then
        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
        TarantoolClientConfig config = client.getConfig();

        assertEquals(1, config.getConnections());
        assertEquals(1000, config.getReadTimeout());
        assertEquals(1000, config.getConnectTimeout());
        assertEquals(2000, config.getRequestTimeout());
        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());
        assertEquals(expectedNumberOfAttempts, actualNumberOfAttempts);
    }

    @Test
    void test_should_createRetryingWithMoreSettingsClient() {
        int expectedDelayMs = 500;
        int expectedNumberOfAttempts = 5;
        int expectedRequestTimeoutMs = 230;
        String expectedUserName = "test";
        String expectedPassword = "passwordTest";
        Function<Throwable, Boolean> expectedCallback = t -> t.getMessage().equals("Test");

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withCredentials(expectedUserName, expectedPassword)
                .withRetryingByNumberOfAttempts(expectedNumberOfAttempts, expectedCallback,
                        policy -> policy.withDelay(expectedDelayMs).withRequestTimeout(expectedRequestTimeoutMs))
                .build();

        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());

        TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?> retryPolicyFactory =
                (TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?>)
                        ((RetryingTarantoolTupleClient) client).getRetryPolicyFactory();
        SimpleTarantoolCredentials credentials = (SimpleTarantoolCredentials) client.getConfig().getCredentials();

        assertEquals(expectedUserName, credentials.getUsername());
        assertEquals(expectedPassword, credentials.getPassword());
        assertEquals(expectedNumberOfAttempts, retryPolicyFactory.getNumberOfAttempts());
        assertEquals(expectedDelayMs, retryPolicyFactory.getDelay());
        assertEquals(expectedCallback, retryPolicyFactory.getExceptionCheck());
        assertEquals(expectedRequestTimeoutMs, retryPolicyFactory.getRequestTimeout());
    }

    @Test
    void test_should_createRetryingWithInfiniteAttemptsClient() {
        //given
        int expectedRetryTimeout = 4444;
        int expectedRetryDelay = 500;
        int expectedOperationTimeout = 123;

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withRetryingIndefinitely(policy -> policy.withDelay(expectedRetryDelay)
                        .withRequestTimeout(expectedRetryTimeout)
                        .withOperationTimeout(expectedOperationTimeout))
                .build();

        //then
        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());

        RequestRetryPolicyFactory policyFactory = ((RetryingTarantoolTupleClient) client).getRetryPolicyFactory();
        assertEquals(TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.class, policyFactory.getClass());
        TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory<?> retryPolicyFactory =
                (TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory<?>) policyFactory;

        assertEquals(expectedRetryDelay, retryPolicyFactory.getDelay());
        assertEquals(expectedRetryTimeout, retryPolicyFactory.getRequestTimeout());
        assertEquals(expectedOperationTimeout, retryPolicyFactory.getOperationTimeout());
    }
}
