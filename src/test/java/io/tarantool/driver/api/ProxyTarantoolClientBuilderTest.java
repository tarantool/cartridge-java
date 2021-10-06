package io.tarantool.driver.api;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static io.tarantool.driver.api.TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN;
import static io.tarantool.driver.api.TarantoolConnectionSelectionStrategyType.ROUND_ROBIN;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProxyTarantoolClientBuilderTest {

    private final TarantoolServerAddress SAMPLE_ADDRESS =
            new TarantoolServerAddress("123.123.123.123", 32323);

    private final TarantoolCredentials SAMPLE_CREDENTIALS =
            new SimpleTarantoolCredentials("root", "passwd");

    private final DefaultMessagePackMapper SAMPLE_MAPPER =
            new DefaultMessagePackMapper.Builder().build();

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
        assertEquals(io.tarantool.driver.api.ProxyTarantoolTupleClient.class, client.getClass());
        TarantoolClientConfig config = client.getConfig();

        assertEquals(1, config.getConnections());
        assertEquals(1000, config.getReadTimeout());
        assertEquals(1000, config.getConnectTimeout());
        assertEquals(2000, config.getRequestTimeout());
        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());

        String deleteFunctionName = ((ProxyTarantoolTupleClient) client).getMappingConfig().getDeleteFunctionName();
        assertEquals(expectedMappedFunctionName, deleteFunctionName);
    }

    @Test
    void test_should_configureProxyClient() {
        //given
        String expectedMappedFunctionName = "mappedFunctionName";

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddresses(SAMPLE_ADDRESS)
                .withCredentials(SAMPLE_CREDENTIALS)
                .withMessagePackMapper(SAMPLE_MAPPER)
                .withProxyMethodMapping(builder -> builder.withDeleteFunctionName(expectedMappedFunctionName))
                .build();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> configuredClient =
                TarantoolClientFactory.configureClient(client).build();

        //then
        assertEquals(ProxyTarantoolTupleClient.class, configuredClient.getClass());
        TarantoolClientConfig config = configuredClient.getConfig();

        assertEquals(1, config.getConnections());
        assertEquals(1000, config.getReadTimeout());
        assertEquals(1000, config.getConnectTimeout());
        assertEquals(2000, config.getRequestTimeout());
        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());

        String deleteFunctionName = ((ProxyTarantoolTupleClient) configuredClient)
                .getMappingConfig().getDeleteFunctionName();
        assertEquals(expectedMappedFunctionName, deleteFunctionName);
    }

    @Test
    void test_should_configureProxyClientAndSetNewParams() {
        //given
        String expectedMappedFunctionName = "crud.delete";

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddresses(SAMPLE_ADDRESS)
                .withCredentials(SAMPLE_CREDENTIALS)
                .withMessagePackMapper(SAMPLE_MAPPER)
                .withProxyMethodMapping(builder -> builder.withDeleteFunctionName(expectedMappedFunctionName))
                .build();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> configuredClient =
                TarantoolClientFactory.configureClient(client)
                        .withProxyMethodMapping()
                        .build();

        //then
        assertEquals(ProxyTarantoolTupleClient.class, configuredClient.getClass());
        TarantoolClientConfig config = configuredClient.getConfig();

        assertEquals(1, config.getConnections());
        assertEquals(1000, config.getReadTimeout());
        assertEquals(1000, config.getConnectTimeout());
        assertEquals(2000, config.getRequestTimeout());
        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());

        String deleteFunctionName = ((ProxyTarantoolTupleClient) configuredClient)
                .getMappingConfig().getDeleteFunctionName();
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
        assertEquals(io.tarantool.driver.api.ProxyTarantoolTupleClient.class, client.getClass());

        String deleteFunctionName = ((ProxyTarantoolTupleClient) client).getMappingConfig().getDeleteFunctionName();
        assertEquals(expectedMappedFunctionName, deleteFunctionName);
    }

    @Test
    void test_should_configureProxyClient_ifDefaultMapping() {
        //given
        String expectedMappedFunctionName = "custom_delete";

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createClient()
                        .withProxyMethodMapping()
                        .build();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> configuredClient =
                TarantoolClientFactory.configureClient(client)
                        .withProxyMethodMapping(mapping -> mapping.withDeleteFunctionName(expectedMappedFunctionName))
                        .build();

        //then
        assertEquals(ProxyTarantoolTupleClient.class, configuredClient.getClass());

        String deleteFunctionName = ((ProxyTarantoolTupleClient) configuredClient)
                .getMappingConfig().getDeleteFunctionName();
        assertEquals(expectedMappedFunctionName, deleteFunctionName);
    }

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
                        .withConnectionSelectionStrategy(RoundRobinStrategyFactory.INSTANCE)
                        .withProxyMethodMapping(builder -> builder
                                .withReplaceFunctionName(expectedReplaceFunctionName)
                                .withTruncateFunctionName(expectedTruncateFunctionName)
                        )
                        .withRetryingByNumberOfAttempts(expectedNumberOfAttempts, expectedCallback,
                                policy -> policy.withDelay(expectedDelay).withRequestTimeout(expectedRequestTimeout))
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
    }
}
