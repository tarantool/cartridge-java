package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.FloatValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@link Float} to {@link FloatValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultFloatToFloatValueConverter implements ObjectConverter<Float, FloatValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public FloatValue toValue(Float object) {
        return ValueFactory.newFloat(object);
    }
}
