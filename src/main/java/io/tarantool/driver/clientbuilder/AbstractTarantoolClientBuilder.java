package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractTarantoolClientBuilder<T, B extends TarantoolClientBuilder<T, B>>
        implements TarantoolClientBuilder<T, B> {

    protected B instance;

    private TarantoolClusterAddressProvider addressProvider;
    private List<TarantoolServerAddress> addressList;
    private TarantoolCredentials credentials;
    private TarantoolClientConfig config;

    public abstract T build();

    public B withCredentials(TarantoolCredentials credentials) {
        this.credentials = credentials;
        return instance;
    }

    public B withCredentials(String username, String password) {
        this.credentials = new SimpleTarantoolCredentials(username, password);
        return instance;
    }

    public B withAddress(TarantoolServerAddress address) {
        initAddressList();
        this.addressList.add(address);
        return instance;
    }

    public B withAddress(String host, int port) {
        initAddressList();
        this.addressList.add(new TarantoolServerAddress(host, port));
        return instance;
    }

    public B withAddressProvider(TarantoolClusterAddressProvider addressProvider) {
        this.addressProvider = addressProvider;
        return instance;
    }

    public B withConfig(TarantoolClientConfig config) {
        this.config = config;
        return instance;
    }

    protected abstract void initBuilder();

    protected TarantoolCredentials getCredentials() {
        return credentials;
    }

    protected TarantoolClusterAddressProvider getAddressProvider() {
        return addressProvider;
    }

    protected TarantoolClientConfig getConfig() {
        return config;
    }

    protected List<TarantoolServerAddress> getAddressList() {
        return addressList;
    }

    private void initAddressList() {
        if (this.addressList == null) {
            this.addressList = new ArrayList<>();
        }
    }
}
