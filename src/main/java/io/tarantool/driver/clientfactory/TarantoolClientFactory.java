package io.tarantool.driver.clientfactory;

import io.tarantool.driver.clientbuilder.AbstractTarantoolClientBuilder;

public interface TarantoolClientFactory {

    AbstractTarantoolClientBuilder createClient();
}
