package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.HashMap;
import java.util.List;
import java.util.function.UnaryOperator;

public class ClientSettingsProviderImpl implements ClientSettingsProvider {

    private final HashMap<String, Object> parameters;

    public ClientSettingsProviderImpl() {
        this.parameters = new HashMap<>();
    }

    @Override
    public ClientSettingsProvider setCredentials(SimpleTarantoolCredentials tarantoolCredentials) {
        parameters.put("credentials", tarantoolCredentials);
        return this;
    }

    @Override
    public ClientSettingsProvider setCredentials(String userName, String password) {
        return null;
    }

    @Override
    public ClientSettingsProvider setAddresses(List<TarantoolServerAddress> address) {
        parameters.put("address", address);
        return this;
    }

    @Override
    public ClientSettingsProvider setConnectionSelectionStrategy(ConnectionSelectionStrategyType type) {
        return null;
    }

    @Override
    public ClientSettingsProvider setMappedCrudMethods(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        parameters.put("proxy", builder);
        return this;
    }

    @Override
    public ClientSettingsProvider setDelay(int delayMs) {
        return null;
    }

    @Override
    public ClientSettingsProvider setRequestTimeout(long timeoutMs) {
        return null;
    }

    @Override
    public ClientSettingsProvider setRetryAttemptsInAmount(int amountOfAttempts) {
        return null;
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return new ClientCreator(parameters).create();
    }
}
