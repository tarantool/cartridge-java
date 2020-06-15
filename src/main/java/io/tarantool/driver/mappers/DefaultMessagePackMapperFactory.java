package io.tarantool.driver.mappers;

import org.msgpack.value.BinaryValue;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.StringValue;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Provides shortcuts for instantiating {@link DefaultMessagePackMapper}
 *
 * @author Alexey Kuzin
 */
public class DefaultMessagePackMapperFactory {

    private static final DefaultMessagePackMapperFactory instance = new DefaultMessagePackMapperFactory();

    private final DefaultMessagePackMapper defaultSimpleTypesMapper;
    private final DefaultMessagePackMapper defaultComplexTypesMapper;

    /**
     * Basic constructor.
     */
    public DefaultMessagePackMapperFactory() {
        defaultSimpleTypesMapper = new DefaultMessagePackMapper.Builder()
                // converters for primitive values
                .withConverter(StringValue.class, String.class, new DefaultStringConverter())
                .withConverter(IntegerValue.class, Integer.class, new DefaultIntegerConverter())
                .withConverter(IntegerValue.class, Long.class, new DefaultLongConverter())
                .withConverter(BinaryValue.class, byte[].class, new DefaultByteArrayConverter())
                .withConverter(BooleanValue.class, Boolean.class, new DefaultBooleanConverter())
                .withConverter(FloatValue.class, Float.class, new DefaultFloatConverter())
                .withConverter(FloatValue.class, Double.class, new DefaultDoubleConverter())
                .withConverter(ExtensionValue.class, UUID.class, new DefaultUUIDConverter())
                .withConverter(ExtensionValue.class, BigDecimal.class, new DefaultBigDecimalConverter())
                .build();
        // internal types converter
        defaultSimpleTypesMapper.registerObjectConverter(new DefaultPackableObjectConverter(defaultSimpleTypesMapper));
        defaultComplexTypesMapper = new DefaultMessagePackMapper.Builder(defaultSimpleTypesMapper)
                .withDefaultListObjectConverter()
                .withDefaultArrayValueConverter()
                .withDefaultMapObjectConverter()
                .withDefaultMapValueConverter()
                .build();
    }

    /**
     * Get modification-safe instance of the mapper. The instance already contains converters for simple types
     * @return {@link DefaultMessagePackMapper} instance
     */
    public DefaultMessagePackMapper defaultSimpleTypeMapper() {
        return new DefaultMessagePackMapper(defaultSimpleTypesMapper);
    }

    /**
     * Get modification-safe instance of the mapper. The instance contains converters for simple types and complex types
     * {@link java.util.Map} and {@link java.util.List}
     * @return {@link DefaultMessagePackMapper} instance
     */
    public DefaultMessagePackMapper defaultComplexTypesMapper() {
        return new DefaultMessagePackMapper(defaultComplexTypesMapper);
    }

    /**
     * Get factory instance.
     * @return factory instance
     */
    public static DefaultMessagePackMapperFactory getInstance() {
        return instance;
    }
}
