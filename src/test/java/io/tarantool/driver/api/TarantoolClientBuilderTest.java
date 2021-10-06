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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for client builder {@link TarantoolClientBuilder}
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientBuilderTest {

    private final TarantoolCredentials SAMPLE_CREDENTIALS =
            new SimpleTarantoolCredentials("root", "passwd");

    private final DefaultMessagePackMapper SAMPLE_MAPPER =
            new DefaultMessagePackMapper.Builder().build();

    private final TarantoolServerAddress SAMPLE_ADDRESS =
            new TarantoolServerAddress("123.123.123.123", 32123);

    private final int SAMPLE_CONNECTIONS = 3;
    private final int SAMPLE_CONNECT_TIMEOUT = 5000;
    private final int SAMPLE_REQUEST_TIMEOUT = 400;
    private final int SAMPLE_READ_TIMEOUT = 4999;

    @Test
    void test_should_createClient() {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddresses(SAMPLE_ADDRESS)
                .withCredentials(SAMPLE_CREDENTIALS)
                .withMessagePackMapper(SAMPLE_MAPPER)
                .withConnections(SAMPLE_CONNECTIONS)
                .withConnectTimeout(SAMPLE_CONNECT_TIMEOUT)
                .withRequestTimeout(SAMPLE_REQUEST_TIMEOUT)
                .withReadTimeout(SAMPLE_READ_TIMEOUT)
                .build();

        //then
        assertClientParams(client);
    }

    @Test
    void test_should_configureClient() {
        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddresses(SAMPLE_ADDRESS)
                .withCredentials(SAMPLE_CREDENTIALS)
                .withMessagePackMapper(SAMPLE_MAPPER)
                .withConnections(SAMPLE_CONNECTIONS)
                .withConnectTimeout(SAMPLE_CONNECT_TIMEOUT)
                .withRequestTimeout(SAMPLE_REQUEST_TIMEOUT)
                .withReadTimeout(SAMPLE_READ_TIMEOUT)
                .build();

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> configuredClient =
                TarantoolClientFactory.configureClient(client).build();

        //then
        assertClientParams(configuredClient);
    }

    private void assertClientParams(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        assertEquals(ClusterTarantoolTupleClient.class, client.getClass());
        TarantoolClientConfig config = client.getConfig();

        assertTrue(((ClusterTarantoolTupleClient) client).getAddressProvider()
                .getAddresses().contains(SAMPLE_ADDRESS));

        assertEquals(SAMPLE_CREDENTIALS, config.getCredentials());
        assertEquals(SAMPLE_MAPPER, config.getMessagePackMapper());
        assertEquals(SAMPLE_CONNECTIONS, config.getConnections());
        assertEquals(SAMPLE_READ_TIMEOUT, config.getReadTimeout());
        assertEquals(SAMPLE_REQUEST_TIMEOUT, config.getRequestTimeout());
        assertEquals(SAMPLE_CONNECT_TIMEOUT, config.getConnectTimeout());
        assertEquals(PARALLEL_ROUND_ROBIN.value(), config.getConnectionSelectionStrategyFactory());
    }
}
