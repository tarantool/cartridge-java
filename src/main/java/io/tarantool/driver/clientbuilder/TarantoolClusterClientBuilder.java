package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.ClusterTarantoolTupleClient;

public interface TarantoolClusterClientBuilder
        extends TarantoolClientBuilder<ClusterTarantoolTupleClient, TarantoolClusterClientBuilder> {

    TarantoolClusterClientBuilder INSTANCE = new TarantoolClusterClientBuilderImpl();
}
