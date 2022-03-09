package io.tarantool.driver.mappers.converters.object;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.BinaryValue;
import org.msgpack.value.ValueFactory;

/**
 * Default {@code byte[]} to {@link BinaryValue} converter
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultByteArrayToBinaryValueConverter implements ObjectConverter<byte[], BinaryValue> {

    private static final long serialVersionUID = 20220418L;

    @Override
    public BinaryValue toValue(byte[] object) {
        return ValueFactory.newBinary(object);
    }

}
