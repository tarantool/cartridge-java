package io.tarantool.driver.api;

import io.tarantool.driver.TarantoolClient;

/**
 * A factory for index query used in select request and other requests to Tarantool server
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexQueryFactory {
    private TarantoolClient client;

    public TarantoolIndexQueryFactory(TarantoolClient client) {
        this.client = client;
    }

    public TarantoolIndexQuery primary() {
        return null;
    }

    public TarantoolIndexQuery byName(String indexName) {
        return null;
    }
}
