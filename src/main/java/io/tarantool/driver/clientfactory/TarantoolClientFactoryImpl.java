package io.tarantool.driver.clientfactory;

public class TarantoolClientFactoryImpl implements TarantoolClientFactory {

    @Override
    public TarantoolClusterClientBuilderDecorator createClient() {
        return new TarantoolClusterClientBuilderDecoratorImpl();
    }
}
