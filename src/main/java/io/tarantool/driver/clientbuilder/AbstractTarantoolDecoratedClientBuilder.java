package io.tarantool.driver.clientbuilder;

public abstract class AbstractTarantoolDecoratedClientBuilder
        <T, B extends TarantoolClientBuilder<T, B>, D>
        extends AbstractTarantoolClientBuilder<T, B>
        implements TarantoolDecoratedClientBuilder<T, B, D> {

    private D decoratedClient;

    @Override
    public B withDecoratedClient(D decoratedClient) {
        this.decoratedClient = decoratedClient;
        return super.instance;
    }

    public D getDecoratedClient() {
        return decoratedClient;
    }
}
