package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.List;
import java.util.function.UnaryOperator;

public interface ClientSettingsProvider {

    ClientSettingsProvider setCredentials(SimpleTarantoolCredentials tarantoolCredentials);

    ClientSettingsProvider setCredentials(String userName, String password);

    ClientSettingsProvider setAddresses(List<TarantoolServerAddress> address);

    ClientSettingsProvider setConnectionSelectionStrategy(ConnectionSelectionStrategyType type);

    ClientSettingsProvider setMappedCrudMethods(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder);

    ClientSettingsProvider setDelay(int delayMs);

    ClientSettingsProvider setRequestTimeout(long timeoutMs);

    ClientSettingsProvider setRetryAttemptsInAmount(int amountOfAttempts);

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
