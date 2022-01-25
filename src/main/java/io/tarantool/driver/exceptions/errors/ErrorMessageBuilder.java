package io.tarantool.driver.exceptions.errors;

import org.msgpack.value.StringValue;
import org.msgpack.value.Value;

import java.util.Map;

public class ErrorMessageBuilder {

    private final StringBuilder stringBuilder;
    private final Map<Value, Value> values;
    private final ErrorKey[] keys;

    public ErrorMessageBuilder(String startsWith, ErrorKey[] keys, Map<Value, Value> values) {
        this.stringBuilder = new StringBuilder(startsWith);
        this.values = values;
        this.keys = keys;
    }

    public String build() {
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
