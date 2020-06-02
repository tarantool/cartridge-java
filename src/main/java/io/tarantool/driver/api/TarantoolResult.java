package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.List;

/**
 * Basic interface for Tarantool operations result -- an array of tuples
 *
 * @author Alexey Kuzin
 */
public interface TarantoolResult extends List<TarantoolTuple> {
}
