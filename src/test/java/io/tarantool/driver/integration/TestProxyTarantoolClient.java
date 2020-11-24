package io.tarantool.driver.integration;

import io.tarantool.driver.ProxyTarantoolClient;
import io.tarantool.driver.api.TarantoolClient;

/**
 * @author Alexey Kuzin
 */
public class TestProxyTarantoolClient extends ProxyTarantoolClient {
    public TestProxyTarantoolClient(TarantoolClient decoratedClient) {
        super(decoratedClient);
    }

    @Override
    public String getGetSchemaFunctionName() {
        return "ddl.get_schema";
    }
}
