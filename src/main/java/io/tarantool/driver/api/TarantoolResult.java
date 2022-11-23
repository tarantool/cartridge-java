package io.tarantool.driver.api;

import java.util.List;

/**
 * Basic interface for Tarantool operations result.
 * It can be multiple return values from Lua, tuple(s) from IProto, and other things in different structures
 *
 * @param <T> target tuple type
 * @author Alexey Kuzin
 */
public interface TarantoolResult<T> extends List<T> {
}
