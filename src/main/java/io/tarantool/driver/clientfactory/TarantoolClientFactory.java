package io.tarantool.driver.clientfactory;

public interface TarantoolClientFactory {

    TarantoolClientFactory INSTANCE = new TarantoolClientFactoryImpl();

    static TarantoolClientFactory getInstance() {
        return INSTANCE;
    }

    TarantoolClusterClientBuilderDecorator createClient();
}
