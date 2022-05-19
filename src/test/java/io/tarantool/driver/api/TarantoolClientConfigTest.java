package io.tarantool.driver.api;

import io.netty.handler.ssl.SslContextBuilder;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Oleg Kuznetsov
 */
public class TarantoolClientConfigTest {

    @Test
    public void test_should_createClientConfigWithSslContext() throws SSLException {
        //given
        final TarantoolServerAddress address = new TarantoolServerAddress("localhost", 3301);
        final SimpleTarantoolCredentials credentials = new SimpleTarantoolCredentials("test", "test");

        //when
        final TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withSslContext(SslContextBuilder.forClient().build())
                .withCredentials(credentials)
                .build();

        final ClusterTarantoolTupleClient clientWithSsl = new ClusterTarantoolTupleClient(config, address);

        //then
        assertNotNull(config.getSslContext());
        assertNotNull(clientWithSsl.getConfig().getSslContext());
    }
}
