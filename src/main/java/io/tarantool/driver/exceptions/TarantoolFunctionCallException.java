package io.tarantool.driver.exceptions;

import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Map;

/**
 * Represents exceptions returned on call operations to Cartridge API (functions return <code>nil, err</code> on error)
 *
 * @author Alexey Kuzin
 */
public class TarantoolFunctionCallException extends TarantoolException {

    private static final StringValue ERROR_MESSAGE = ValueFactory.newString("str");
    private static final StringValue STACKTRACE = ValueFactory.newString("stack");

    private String errorMessage;
    private String stacktrace;

    public TarantoolFunctionCallException(String errorMessage) {
        super(errorMessage);
        this.errorMessage = errorMessage;
    }

    public TarantoolFunctionCallException(MapValue value) {
        super();
        Map<Value, Value> error = value.map();
        this.errorMessage = error.get(ERROR_MESSAGE).toString();
        this.stacktrace = error.containsKey(STACKTRACE) ? error.get(STACKTRACE).toString() : null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TarantoolFunctionCallException: ");
        sb.append(errorMessage);
        if (stacktrace != null) {
            sb.append("\n").append(stacktrace);
        }
        return sb.toString();
    }
}
