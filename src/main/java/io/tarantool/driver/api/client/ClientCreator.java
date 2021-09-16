package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.HashMap;
import java.util.List;
import java.util.function.UnaryOperator;

public class ClientCreator {

    private final HashMap<String, Object> settings;

    public ClientCreator(HashMap<String, Object> settings) {
        this.settings = settings;
    }

    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> create() {
        List<TarantoolServerAddress> address = (List<TarantoolServerAddress>) settings.get("address");
        TarantoolCredentials credentials = (SimpleTarantoolCredentials) settings.get("credentials");
        UnaryOperator<ProxyOperationsMappingConfig.Builder> proxy = (UnaryOperator<ProxyOperationsMappingConfig.Builder>) settings.get("proxy");

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = new ClusterTarantoolTupleClient();

        if (address != null && credentials != null) {
            client = new ClusterTarantoolTupleClient(credentials, address);
            if (proxy != null) {
                client = new ProxyTarantoolTupleClient(client, proxy.apply(ProxyOperationsMappingConfig.builder()).build());
            }
        }

        return client;
    }
}
