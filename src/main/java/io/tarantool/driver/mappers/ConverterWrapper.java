package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.Converter;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import io.tarantool.driver.mappers.converters.ValueConverter;

import java.io.Serializable;

/**
 * Base class for the internal logic of search converters.
 * It is needed for faster possibility of obtaining a target type of converter.
 * Target type is the second type parameter in {@link ValueConverter} or {@link ObjectConverter}.
 *
 * @param <C> the converter that returns an instance of this target type from an instance of input type
 * @author Artyom Dubinin
 */
class ConverterWrapper<C extends Converter> implements Serializable {

    private static final long serialVersionUID = 20220501L;

    private final C converter;
    private final Class<?> targetClass;

    ConverterWrapper(C converter, Class<?> targetClass) {
        this.converter = converter;
        this.targetClass = targetClass;
    }

    public C getConverter() {
        return converter;
    }

    public Class<?> getTargetClass() {
        return targetClass;
    }
}
