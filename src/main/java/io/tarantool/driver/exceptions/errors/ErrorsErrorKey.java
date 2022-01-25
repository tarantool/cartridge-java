package io.tarantool.driver.exceptions.errors;

import org.msgpack.value.StringValue;
import org.msgpack.value.ValueFactory;

public enum ErrorsErrorKey implements ErrorKey {
    LINE("line", ValueFactory.newString("line")),
    CLASS_NAME("class_name", ValueFactory.newString("line")),
    ERR("err", ValueFactory.newString("err")),
    FILE("file", ValueFactory.newString("file")),
    ERROR_MESSAGE("str", ValueFactory.newString("str")),
    STACKTRACE("stack", ValueFactory.newString("stack"));

    private final String key;
    private final StringValue msgPackKey;

    ErrorsErrorKey(String key, StringValue msgPackKey) {
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
