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
 * @author Artyom Dubinin
 */
public final class DefaultMessagePackMapperFactory {

    private static final DefaultMessagePackMapperFactory instance = new DefaultMessagePackMapperFactory();

    private final DefaultMessagePackMapper defaultSimpleTypesMapper;

    /**
     * Basic constructor.
     */
    private DefaultMessagePackMapperFactory() {
        defaultSimpleTypesMapper = new DefaultMessagePackMapper.Builder()
                // converters for primitive values
                .withConverter(StringValue.class, String.class, new DefaultStringConverter())
                .withValueConverter(IntegerValue.class, Float.class, new DefaultIntegerValueToFloatConverter())
                .withValueConverter(IntegerValue.class, Double.class, new DefaultIntegerValueToDoubleConverter())
                .withConverter(IntegerValue.class, Long.class, new DefaultLongConverter())
                .withConverter(IntegerValue.class, Integer.class, new DefaultIntegerConverter())
                .withConverter(BinaryValue.class, byte[].class, new DefaultByteArrayConverter())
                .withConverter(BooleanValue.class, Boolean.class, new DefaultBooleanConverter())
                .withConverter(FloatValue.class, Double.class, new DefaultDoubleConverter())
                .withConverter(FloatValue.class, Float.class, new DefaultFloatConverter())
                .withConverter(ExtensionValue.class, UUID.class, new DefaultUUIDConverter())
                .withConverter(ExtensionValue.class, BigDecimal.class, new DefaultBigDecimalConverter())
                .withValueConverter(new DefaultNilConverter())
                .build();
    }

    /**
     * Get modification-safe instance of the mapper. The instance already contains converters for simple types
     * @return {@link DefaultMessagePackMapper} instance
     */
    public DefaultMessagePackMapper defaultSimpleTypeMapper() {
        // internal types converter
        DefaultMessagePackMapper simpleTypesMapper = new DefaultMessagePackMapper(defaultSimpleTypesMapper);
        simpleTypesMapper.registerObjectConverter(
                new DefaultPackableObjectConverter(simpleTypesMapper));
        return simpleTypesMapper;
    }

    /**
     * Get modification-safe instance of the mapper. The instance contains converters for simple types and complex types
     * {@link java.util.Map} and {@link java.util.List}
     * @return {@link DefaultMessagePackMapper} instance
     */
    public DefaultMessagePackMapper defaultComplexTypesMapper() {
        DefaultMessagePackMapper defaultComplexTypesMapper =
                new DefaultMessagePackMapper.Builder(defaultSimpleTypesMapper)
                .withDefaultListObjectConverter()
                .withDefaultArrayValueConverter()
                .withDefaultMapObjectConverter()
                .withDefaultMapValueConverter()
                .build();

        // internal types converter
        defaultComplexTypesMapper.registerObjectConverter(
                new DefaultPackableObjectConverter(defaultComplexTypesMapper));
        return defaultComplexTypesMapper;
    }

    /**
     * Get modification-safe instance of the given mapper (shallow copy).
     * @param mapper configured mapper instance
     * @return new mapper instance
     */
    public DefaultMessagePackMapper copyOf(DefaultMessagePackMapper mapper) {
        return new DefaultMessagePackMapper(mapper);
    }

    /**
     * Get new empty mapper.
     * @return new mapper instance
     */
    public DefaultMessagePackMapper emptyMapper() {
        return new DefaultMessagePackMapper();
    }

    /**
     * Get factory instance.
     * @return factory instance
     */
    public static DefaultMessagePackMapperFactory getInstance() {
        return instance;
    }
}
