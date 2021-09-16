package io.tarantool.driver.api.client;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClientFactoryTest {

    @Test
    void test_should_createClient() {
        List<TarantoolServerAddress> addressList = Collections.singletonList(new TarantoolServerAddress("123.123.123.123", 3301));

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                ClientFactory.createClient()
                        .setMappedCrudMethods((b) -> b.withDeleteFunctionName("createTest"))
                        .setMappedCrudMethods((b) -> b.withDeleteFunctionName("TESTTEST"))
                        .setCredentials(new SimpleTarantoolCredentials("root", "passwd"))
                        .setAddresses(addressList)
                        .build();

        assertEquals(ProxyTarantoolTupleClient.class, client.getClass());
    }
}
