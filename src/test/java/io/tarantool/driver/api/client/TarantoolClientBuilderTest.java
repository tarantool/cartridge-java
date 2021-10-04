package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.tarantool.driver.api.client.TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Unit tests for client builder {@link TarantoolClientBuilder}
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientBuilderTest {

    private final TarantoolServerAddress SAMPLE_ADDRESS =
            new TarantoolServerAddress("123.123.123.123", 32323);

    private final TarantoolCredentials SAMPLE_CREDENTIALS =
            new SimpleTarantoolCredentials("root", "passwd");

    private final DefaultMessagePackMapper SAMPLE_MAPPER =
            new DefaultMessagePackMapper.Builder().build();

    @Test
    void test_should_createClient() {
        //given
        int expectedConnections = 3;
        int expectedConnectTimeout = 5000;
        int expectedRequestTimeout = 400;
        int expectedReadTimeout = 4999;

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddresses(SAMPLE_ADDRESS)
                .withCredentials(SAMPLE_CREDENTIALS)
                .withMessagePackMapper(SAMPLE_MAPPER)
                .withConnections(expectedConnections)
                .withConnectTimeout(expectedConnectTimeout)
                .withRequestTimeout(expectedRequestTimeout)
                .withReadTimeout(expectedReadTimeout)
                .build();

        //then
        assertEquals(ClusterTarantoolTupleClient.class, client.getClass());
        TarantoolClientConfig config = client.getConfig();

        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(expectedConnections, config.getConnections());
        assertEquals(expectedReadTimeout, config.getReadTimeout());
        assertEquals(expectedRequestTimeout, config.getRequestTimeout());
        assertEquals(expectedConnectTimeout, config.getConnectTimeout());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());
    }

    @Test
    void test_should_createProxyClient() {
        //given
        String expectedMappedFunctionName = "mappedFunctionName";

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddresses(SAMPLE_ADDRESS)
                .withCredentials(SAMPLE_CREDENTIALS)
                .withMessagePackMapper(SAMPLE_MAPPER)
                .withProxyMethodMapping(builder -> builder.withDeleteFunctionName(expectedMappedFunctionName))
                .build();

        //then
        assertEquals(ProxyTarantoolTupleClient.class, client.getClass());
        assertDefaultBaseParameters(client);

        String deleteFunctionName = ((ProxyTarantoolTupleClient) client).getMappingConfig().getDeleteFunctionName();
        assertEquals(expectedMappedFunctionName, deleteFunctionName);
    }

    @Test
    void test_should_createProxyClient_ifDefaultMapping() {
        //given
        String expectedMappedFunctionName = "crud.delete";

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withProxyMethodMapping()
                .build();

        //then
        assertEquals(ProxyTarantoolTupleClient.class, client.getClass());

        String deleteFunctionName = ((ProxyTarantoolTupleClient) client).getMappingConfig().getDeleteFunctionName();
        assertEquals(expectedMappedFunctionName, deleteFunctionName);
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
                .withRequestRetryAttempts(expectedNumberOfAttempts)
                .build();

        int actualNumberOfAttempts = ((TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory<?>)
                ((RetryingTarantoolTupleClient) client).getRetryPolicyFactory()).getNumberOfAttempts();

        //then
        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
        assertDefaultBaseParameters(client);
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
                .withRequestRetryAttempts(expectedNumberOfAttempts)
                .withRequestRetryDelay(expectedDelayMs)
                .withRequestRetryTimeout(expectedRequestTimeoutMs)
                .withRequestRetryExceptionCallback(expectedCallback)
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
                .withRequestRetryDelay(expectedRetryDelay)
                .withRequestRetryTimeout(expectedRetryTimeout)
                .withRequestRetryOperationTimeout(expectedOperationTimeout)
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

    private void assertDefaultBaseParameters(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        TarantoolClientConfig config = client.getConfig();

        assertEquals(1, config.getConnections());
        assertEquals(1000, config.getReadTimeout());
        assertEquals(1000, config.getConnectTimeout());
        assertEquals(2000, config.getRequestTimeout());
        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());
    }
}
