package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static io.tarantool.driver.api.client.ConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN;
import static io.tarantool.driver.api.client.ConnectionSelectionStrategyType.ROUND_ROBIN;
import static io.tarantool.driver.api.client.ConnectionSelectionStrategyType.defaultType;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClientBuilderTest {

    @Test
    void test_should_clientFactory_createRetryingProxyClient() {
        //given, when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createClientTo("123.123.123.1", 3301)
                        .withCredentials("admin", "testpasswd")
                        .withConnectionSelectionStrategy(defaultType())
                        .withMappedCrudMethods(getMapping())
                        .withRetryAttemptsInAmount(5)
                        .withDelay(200)
                        .withRequestTimeout(3000)
                        .build();

        //then
        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_clientFactory_createProxyClusterClient() {
        //given, when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createClientTo(
                        new TarantoolServerAddress("123.123.123.1", 3301),
                        new TarantoolServerAddress("123.123.123.1", 3302),
                        new TarantoolServerAddress("123.123.123.1", 3303)
                )
                        .withCredentials("admin", "testpasswd")
                        .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
                        .withMappedCrudMethods((builder) -> builder
                                .withDeleteFunctionName("create")
                                .withSchemaFunctionName("delete"))
                        .build();

        //then
        assertEquals(ProxyTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_clientFactory_createClusterClient() {
        //given
        List<TarantoolServerAddress> addressList = new ArrayList<>();
        addressList.add(new TarantoolServerAddress("123.123.123.1", 3301));
        addressList.add(new TarantoolServerAddress("123.123.123.1", 3302));
        addressList.add(new TarantoolServerAddress("123.123.123.1", 3303));
        addressList.add(new TarantoolServerAddress("123.123.123.1", 3304));

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createClientTo(addressList)
                        .withCredentials("admin", "testpasswd")
                        .withConnectionSelectionStrategy(ROUND_ROBIN)
                        .build();

        //then
        assertEquals(ClusterTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_clientFactory_createDefaultClusterClient() {
        //given, when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createClient()
                        .withDefaultCredentials()
                        .withDefaultConnectionSelectionStrategy()
                        .build();

        //then
        assertEquals(ClusterTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_clientFactory_createDefaultClusterClientWithShortcut() {
        //given, when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                TarantoolClientFactory.createDefaultClient();

        //then
        assertEquals(ClusterTarantoolTupleClient.class, client.getClass());
    }

    private static UnaryOperator<ProxyOperationsMappingConfig.Builder> getMapping() {
        return (builder) -> builder
                .withDeleteFunctionName("delete")
                .withSchemaFunctionName("test");
    }
}
