package io.tarantool.driver.exceptions.errors;

import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

public enum BoxErrorKey implements ErrorKey {
    CODE("code", ValueFactory.newString("code")),
    BASE_TYPE("base_type", ValueFactory.newString("base_type")),
    TYPE("type", ValueFactory.newString("type")),
    MESSAGE("message", ValueFactory.newString("message")),
    TRACE("trace", ValueFactory.newString("trace"));

    private final String key;
    private final StringValue msgPackKey;

    BoxErrorKey(String key, StringValue msgPackKey) {
        this.key = key;
        this.msgPackKey = msgPackKey;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public StringValue getMsgPackKey() {
        return msgPackKey;
    }
}
