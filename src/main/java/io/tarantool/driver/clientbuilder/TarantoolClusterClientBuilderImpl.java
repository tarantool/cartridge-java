package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.ClusterTarantoolTupleClient;

class TarantoolClusterClientBuilderImpl
        extends AbstractTarantoolClientBuilder<ClusterTarantoolTupleClient, TarantoolClusterClientBuilder>
        implements TarantoolClusterClientBuilder {

    public TarantoolClusterClientBuilderImpl() {
        initBuilder();
    }

    @Override
    public ClusterTarantoolTupleClient build() {
        if (super.getAddressProvider() != null && super.getConfig() != null) {
            return new ClusterTarantoolTupleClient(super.getConfig(), super.getAddressProvider());
        }

        if (super.getAddressList() != null && super.getConfig() != null) {
            return new ClusterTarantoolTupleClient(super.getConfig(), super.getAddressList());
        }

        if (super.getCredentials() != null && super.getAddressList() != null) {
            return new ClusterTarantoolTupleClient(super.getCredentials(), getAddressList());
        }

        if (super.getCredentials() != null) {
            return new ClusterTarantoolTupleClient(super.getCredentials());
        }

        if (super.getAddressList() != null) {
            return new ClusterTarantoolTupleClient(super.getAddressList());
        }

        return new ClusterTarantoolTupleClient();
    }

    @Override
    protected void initBuilder() {
        super.instance = this;
    }
}
