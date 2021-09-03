package io.tarantool.driver.clientbuilder;

public interface TarantoolDecoratedClientBuilder<T, B extends TarantoolClientBuilder<T, B>, D>
        extends TarantoolClientBuilder<T, B> {

    B withDecoratedClient(D client);

}
