package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;

/**
 * Provides factory that creates result mappers for parsing {@code TarantoolResult<TarantoolTuple>} from various
 * structures
 *
 * @author Artyom Dubinin
 */
public interface TarantoolTupleResultMapperFactory {
    /**
     * Mapper parses result from a list of tuples.
     * Use this mapper for handling containers with {@link TarantoolTuple} inside.
     * The IProto method results by default contain lists of tuples.
     * This mapper can be used at the inner level by other mapper factories that are handling higher-level containers.
     * <p>
     * input: array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>}
     * <p>
     * Returned TarantoolResult doesn't store metadata, and you can't get field by field name
     *
     * @return mapper that parses list of arrays to {@code TarantoolResult<TarantoolTuple>}
     */
    TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper);

    /**
     * Mapper parses result from a list of tuples.
     * Use this mapper for handling containers with {@link TarantoolTuple} inside.
     * The IProto method results by default contain lists of tuples.
     * This mapper can be used at the inner level by other mapper factories that are handling higher-level containers.
     * <p>
     * input: array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>}
     * <p>
     * For example, it can be used to parse result from IPROTO_SELECT
     *
     * @return mapper that parses list of arrays to {@code TarantoolResult<TarantoolTuple>}
     */
    TarantoolResultMapper<TarantoolTuple> withArrayValueToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata);

    /**
     * Mapper parses result from a map with list of tuples and metadata.
     * Use this mapper for handling containers with {@link TarantoolTuple} inside.
     * The crud method results by default contain map with list of tuples and metadata.
     * This mapper can be used at the inner level by other mapper factories that are handling higher-level containers.
     * <p>
     * input: map with metadata and array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>}
     * <p>
     * Returned TarantoolResult doesn't store metadata, and you can't get field by field name
     *
     * @return mapper that parses map to {@code TarantoolResult<TarantoolTuple>}
     */
    TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper);

    /**
     * Mapper parses result from a map with list of tuples and metadata.
     * Use this mapper for handling containers with {@link TarantoolTuple} inside.
     * The crud method results by default contain map with list of tuples and metadata.
     * This mapper can be used at the inner level by other mapper factories that are handling higher-level containers.
     * <p>
     * input: map with metadata and array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>}
     * <p>
     *
     * @return mapper that parses map to {@code TarantoolResult<TarantoolTuple>}
     */
    TarantoolResultMapper<TarantoolTuple> withRowsMetadataToTarantoolTupleResultConverter(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata);

    /**
     * Mapper for single the stored Lua function call result in the form <code>return result, err</code>
     * with a list of tuples as a result.
     * For example, this form of the result is used for some return from lua function like box.space.select
     * <p>
     * input: [x, y, ...], MessagePack array from a Lua function multi-return response
     * <br>
     * where <code>x</code> is a data structure with an array of tuples inside ([t1, t2, ...]) and <code>y</code>
     * can be interpreted as an error structure if it is not empty and there are no more arguments after
     * <code>y</code>`
     * <br>
     * mapper result: converted value of <code>x</code> to {@code TarantoolResult<TarantoolTuple>}
     * <p>
     * Mapper result and the inner contents depend on the parameters or the passed converters.
     *
     * @return default mapper for single value call result with a list of tuples
     */
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withSingleValueArrayToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata);

    /**
     * Mapper for single the stored Lua function call result in the form <code>return result, err</code>
     * with a list of tuples as a result.
     * For example, this form of the result is used for some return from lua function like crud.select
     * <p>
     * input: [x, y, ...], MessagePack array from a Lua function multi-return response
     * <br>
     * where <code>x</code> map with metadata and array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: converted value of <code>x</code> to {@code TarantoolResult<TarantoolTuple>}
     * <p>
     *
     * @return mapper for single value call result with a list of tuples
     */
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    withSingleValueRowsMetadataToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata);

    /**
     * Mapper for the stored Lua function call result, interpreted in a way that each returned item is a tuple.
     * Use this factory for handling proxy function call results which return tuples as a multi-return result.
     * <p>
     * input: [x, y, z, ...], MessagePack array from a Lua function multi-return response
     * <br>
     * where <code>x, y, z</code> are some MessagePack values, each one representing a Tarantool tuple
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>} converted from <code>[x, y, z, ...]</code>
     * <p>
     * Mapper result and its inner contents depend on the parameters or the passed converters.
     *
     * @return mapper for multi value call result as a list of tuples
     */
    CallResultMapper<
        TarantoolResult<TarantoolTuple>,
        MultiValueCallResult<TarantoolTuple, TarantoolResult<TarantoolTuple>>>
    withMultiValueArrayToTarantoolTupleResultMapper(
        MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata);
}
