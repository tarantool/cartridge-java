package io.tarantool.driver.api.space.options.crud.enums;

/**
 * General enumeration class representing the names of CRUD options.
 *
 * @author <a href="https://github.com/nickkkccc">Belonogov Nikolay</a>
 */
public enum ProxyOption {

    MODE("mode"),

    ROLLBACK_ON_ERROR("rollback_on_error"),

    STOP_ON_ERROR("stop_on_error"),

    TIMEOUT("timeout"),

    FIELDS("fields"),

    BUCKET_ID("bucket_id"),

    BATCH_SIZE("batch_size"),

    AFTER("after"),

    FIRST("first"),

    YIELD_EVERY("yield_every"),

    FORCE_MAP_CALL("force_map_call");

    private final String name;

    ProxyOption(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
