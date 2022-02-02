package io.tarantool.driver.exceptions.errors;

import org.msgpack.value.StringValue;
import org.msgpack.value.Value;

import java.util.Map;

/**
 * This class can create error message by keys from msgPack error response
 *
 * @author Oleg Kuznetsov
 */
class ErrorMessageBuilder {

    private final StringBuilder stringBuilder;
    private final Map<Value, Value> values;
    private final ErrorKey[] keys;

    ErrorMessageBuilder(String startsWith, ErrorKey[] keys, Map<Value, Value> values) {
        this.stringBuilder = new StringBuilder(startsWith);
        this.values = values;
        this.keys = keys;
    }

    String build() {
        for (ErrorKey key : keys) {
            final StringValue stringValue = key.getMsgPackKey();
            final String s = values.containsKey(stringValue) ? values.get(stringValue).toString() : null;
            if (s != null && s.length() != 0) {
                stringBuilder.append("\n").append(key.getKey().toLowerCase()).append(": ").append(s);
            }
        }

        return stringBuilder.toString();
    }
}
