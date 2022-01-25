package io.tarantool.driver.exceptions.errors;

import org.msgpack.value.StringValue;

public interface ErrorKey {
    String getKey();

    StringValue getMsgPackKey();
}
