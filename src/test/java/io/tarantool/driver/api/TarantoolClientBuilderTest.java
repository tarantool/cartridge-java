package io.tarantool.driver.api;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import org.junit.jupiter.api.Test;

import static io.tarantool.driver.api.TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN;
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
    void test_should_configureClient() {
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

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> configuredClient =
                TarantoolClientFactory.configureClient(client).build();

        //then
        assertEquals(ClusterTarantoolTupleClient.class, configuredClient.getClass());
        TarantoolClientConfig config = configuredClient.getConfig();

        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(expectedConnections, config.getConnections());
        assertEquals(expectedReadTimeout, config.getReadTimeout());
        assertEquals(expectedRequestTimeout, config.getRequestTimeout());
        assertEquals(expectedConnectTimeout, config.getConnectTimeout());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());
    }
}
