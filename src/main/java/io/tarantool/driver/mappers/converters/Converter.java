package io.tarantool.driver.mappers.converters;

import java.io.Serializable;

/**
 * Basic interface for converters mapping MessagePack entities and Java objects.
 * It must contain a method that maps the value of input type to the value of target type.
 *
 * @author Artyom Dubinin
 */
public interface Converter extends Serializable {

}
