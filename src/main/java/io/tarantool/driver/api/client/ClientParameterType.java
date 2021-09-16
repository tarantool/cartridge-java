package io.tarantool.driver.api.client;

public enum ClientParameterType {
    CREDENTIALS(0),
    ADDRESS(1),
    PROXY(2),
    RETRY(3);

    private final int order;

    ClientParameterType(int order) {
        this.order = order;
    }

    public int getOrder() {
        return this.order;
    }
}
