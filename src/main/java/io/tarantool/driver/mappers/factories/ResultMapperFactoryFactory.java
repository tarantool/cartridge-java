package io.tarantool.driver.mappers.factories;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleResult;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactory;

import java.util.List;

/**
 * Provides different factories for creating result mappers
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public interface ResultMapperFactoryFactory {
    /**
     * Default factory for call result with a list of tuples.
     * Use this factory for handling containers with {@link TarantoolTuple} inside.
     * The IProto method results by default contain lists of tuples.
     * This factory can be used at the inner level by other mapper factories that are handling higher-level containers.
     * <p>
     * input: array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>}
     * <p>
     * Mapper result and its inner depends on the parameters or converters you passed.
     *
     * @return default factory for list of tuples results
     */
    ArrayValueToTarantoolTupleResultMapperFactory arrayTupleResultMapperFactory();

    /**
     * Default factory for call result with different structures.
     * Use this factory for handling containers with {@link TarantoolTuple} inside.
     * The IProto method results by default contain lists of tuples.
     * The crud results by default contain map with list of tuple inside.
     * This factory can be used at the inner level by other mapper factories that are handling higher-level containers.
     * <p>
     * input: structure with array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>}
     * <p>
     *
     * @return default factory for list of tuples results
     */
    TarantoolTupleResultMapperFactory getTarantoolTupleResultMapperFactory();

    /**
     * Default factory for call result with a list of tuples in structure with metadata.
     * Use this factory for handling containers with {@link TarantoolTuple} inside.
     * The IProto method results by default contain lists of tuples.
     * This factory can be used at the inner level by other mapper factories that are handling higher-level containers.
     * <p>
     * input: map with metadata and array of tuples with MessagePack values inside ([t1, t2, ...])
     * <br>
     * mapper result: {@code TarantoolResult<TarantoolTuple>}
     * <p>
     * Mapper result and its inner depends on the parameters or converters you passed.
     *
     * @return default factory for list of tuples results
     */
    RowsMetadataToTarantoolTupleResultMapperFactory rowsMetadataTupleResultMapperFactory();

    /**
     * Default factory for single the stored Lua function call result in the form <code>return result, err</code>
     * with a list of tuples as a result.
     * For example, this form of the result is used for some tarantool/crud library responses.
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
     * @return default factory for single value call result with a list of tuples
     */
    SingleValueWithTarantoolTupleResultMapperFactory singleValueTupleResultMapperFactory();

    /**
     * Create a factory for mapping stored function call result to {@link SingleValueCallResult} containing a list
     * of tuples mapped to {@link TarantoolResult}
     * <p>
     * input: [x, y, z], MessagePack array from a Lua function multi return response
     * <br>
     * where <code>x</code> is a data structure with an array of tuples inside ([t1, t2, ...]) and <code>y</code>
     * can be interpreted as an error structure if it is not empty and there are no more arguments after <code>y</code>.
     * A tuple can be represented by any custom object or value.
     * <br>
     * mapper result: converted value of <code>x</code> to {@code TarantoolResult<TarantoolTuple>}
     * <p>
     * Mapper result and its inner contents depend on the parameters or the passed converters.
     *
     * @param <T> target inner content type
     * @return new or existing factory instance
     */
    <T> SingleValueWithTarantoolResultMapperFactory<T> singleValueTarantoolResultMapperFactory();

    /**
     * Create a factory for mapping stored Lua function call results to {@link SingleValueCallResult}
     * <p>
     * input: [x, y, ...], MessagePack array from a Lua function multi-return response
     * <br>
     * where <code>x, y</code> are some MessagePack values. <code>x</code> is interpreted as a stored Lua function
     * result and <code>y</code> may be interpreted as a stored Lua function error if it is not empty and there are
     * no more arguments after <code>y</code>.
     * <br>
     * mapper result: some Java type converted from the value of <code>x</code> according to the mapper, passed as a
     * parameter to this one for parsing the result contents.
     * <p>
     * Mapper result and its inner contents depend on the parameters or the passed converters.
     *
     * @param <T> target result type
     * @return new or existing factory instance
     */
    <T> SingleValueResultMapperFactory<T> singleValueResultMapperFactory();

    /**
     * Default factory for the stored Lua function call result, interpreted in a way that each returned item is a tuple.
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
     * @return new or existing factory instance
     */
    MultiValueWithTarantoolTupleResultMapperFactory multiValueTupleResultMapperFactory();

    /**
     * Default factory for the mapping stored Lua function call multi-return result to {@link TarantoolResult}, where
     * each value is interpreted as a tuple of custom type.
     * <p>
     * input: [x, y, z, ...], MessagePack array from a Lua function multi-return response
     * <br>
     * where <code>x, y, z</code> are some MessagePack values, each one representing a custom value
     * <br>
     * mapper result: {@code TarantoolResult<T>} converted from <code>[x, y, z, ...]</code>
     * <p>
     * Mapper result and its inner contents depend on the parameters or the passed converters.
     * Mapper checks errors and that result is not empty and uses
     * {@link ArrayValueToTarantoolResultMapperFactory} for further parsing
     *
     * @param <T> target inner content type
     * @return new or existing factory instance
     */
    <T> MultiValueWithTarantoolResultMapperFactory<T> multiValueTarantoolResultMapperFactory();

    /**
     * Create a factory for mapping stored Lua function multi-return call result to {@link MultiValueCallResult} with
     * the custom result and tuple types.
     * <p>
     * input: [x, y, z, ...], MessagePack array from a Lua function multi-return response that is converted to the
     * target result type
     * <br>
     * where <code>x, y, z</code> are some MessagePack values, each one representing a custom value
     * <br>
     * mapper result: a custom Java type inheriting {@code List<T>} that is converted from <code>[x, y, z, ...]</code>
     * <p>
     * Mapper result and its inner contents depend on the parameters or the passed converters.
     *
     * @param <T> target result type
     * @param <R> target result inner contents item type
     * @return new or existing factory instance
     */
    <T, R extends List<T>> MultiValueResultMapperFactory<T, R> multiValueResultMapperFactory();

    /**
     * Create a factory for mapping stored function call result to {@link MultiValueCallResult} containing a list
     * of tuples mapped to {@link TarantoolResult}
     * <p>
     * input: storing structure(e.g. [x, y, z] messagepack array)
     * <br>
     * where x, y, z are some MessagePack Values
     * <br>
     * mapper result: converted {@code TarantoolResult<T>} from [x, y, z]
     * <p>
     * Mapper result and its inner contents depend on the parameters or the passed converters.
     * For example, you can parse any MessagePack array to TarantoolResult even if it's an empty array.
     *
     * @param <T> target inner result type
     * @return new or existing factory instance
     */
    <T> ArrayValueToTarantoolResultMapperFactory<T> rowsMetadataStructureResultMapperFactory();

    /**
     * Return builder to create mapper which may depend on input clientMapper
     * For example, you can create maper that can obtain crud and box results from lua
     * and read it into {@link TarantoolTupleResult} structure.
     * <pre>
     * <code>
     *     CallResultMapper callReturnMapper = factory.createMapper(valueMapper)
     *             .withSingleValueConverter( // obtain first value from lua multi-value response
     *                 factory.createMapper(valueMapper)
     *                     .withArrayValueToTarantoolTupleResultConverter() // box type response
     *                     .withRowsMetadataToTarantoolTupleResultMapper() // crud type response
     *                     .buildCallResultMapper()
     *             )
     *             .buildCallResultMapper();
     * </code>
     * </pre>
     * This mapper wouldn't have metadata to take fields by their names.
     *
     * @param messagePackMapper input client mapper
     * @return Builder to create mapper with specific converters inside
     */
    Builder createMapper(MessagePackMapper messagePackMapper);

    /**
     * Return builder to create mapper which may depend on input clientMapper
     * For example, you can create maper that can obtain crud and box results from lua
     * and read it into {@link TarantoolTupleResult} structure.
     * <pre>
     * <code>
     *     CallResultMapper callReturnMapper = factory.createMapper(valueMapper)
     *             .withSingleValueConverter( // obtain first value from lua multi-value response
     *                 factory.createMapper(valueMapper)
     *                     .withArrayValueToTarantoolTupleResultConverter() // box type response
     *                     .withRowsMetadataToTarantoolTupleResultMapper() // crud type response
     *                     .buildCallResultMapper()
     *             )
     *             .buildCallResultMapper();
     * </code>
     * </pre>
     *
     * @param messagePackMapper input client mapper
     * @param spaceMetadata metadata to get fields by names
     * @return Builder to create mapper with specific converters inside
     */
    Builder createMapper(MessagePackMapper messagePackMapper, TarantoolSpaceMetadata spaceMetadata);

    interface Builder {

        /**
         * Add a converter for mapping stored Lua function call results to {@link SingleValueCallResult}
         * <p>
         * input: [x, y, ...], MessagePack array from a Lua function multi-return response
         * <br>
         * where <code>x, y</code> are some MessagePack values. <code>x</code> is interpreted as a stored Lua function
         * result and <code>y</code> may be interpreted as a stored Lua function error if it is not empty and there are
         * no more arguments after <code>y</code>.
         * <br>
         * mapper result: some Java type converted from the value of <code>x</code> according to the mapper, passed as a
         * parameter to builder constructor for parsing the result contents.
         * <p>
         * Converter result and its inner contents depend on the parameters or the passed converters.
         *
         * @return builder with single value converter
         */
        Builder withSingleValueConverter();

        /**
         * Add a converter for mapping stored Lua function call results to {@link SingleValueCallResult}
         * <p>
         * input: [x, y, ...], MessagePack array from a Lua function multi-return response
         * <br>
         * where <code>x, y</code> are some MessagePack values. <code>x</code> is interpreted as a stored Lua function
         * result and <code>y</code> may be interpreted as a stored Lua function error if it is not empty and there are
         * no more arguments after <code>y</code>.
         * <br>
         * mapper result: some Java type converted from the value of <code>x</code> according to the mapper, passed as a
         * parameter to this one for parsing the result contents.
         * <p>
         * Converter result and its inner contents depend on the parameters or the passed converters.
         *
         * @param messagePackMapper to parse fields
         * @return builder with single value converter
         */
        Builder withSingleValueConverter(
            MessagePackValueMapper messagePackMapper);

        /**
         * Add converter parses result from a list of tuples.
         * Use this converter for handling containers with {@link TarantoolTuple} inside.
         * The IProto method results by default contain lists of tuples.
         * This converter can be used at the inner level by other mapper factories that are handling higher-level
         * containers.
         * <p>
         * input: array of tuples with MessagePack values inside ([t1, t2, ...])
         * <br>
         * converter result: {@code TarantoolResult<TarantoolTuple>}
         * <p>
         * For example, it can be used to parse result from IPROTO_SELECT.
         * fields result: some Java type converted from the value of <code>x</code> according to the mapper, passed as a
         * parameter to builder constructor for parsing the result contents.
         *
         * @return builder with converter that parses list of arrays to {@code TarantoolResult<TarantoolTuple>}
         */
        Builder withArrayValueToTarantoolTupleResultConverter();

        /**
         * Add converter parses result from a list of tuples.
         * Use this converter for handling containers with {@link TarantoolTuple} inside.
         * The IProto method results by default contain lists of tuples.
         * This converter can be used at the inner level by other mapper factories that are handling higher-level
         * containers.
         * <p>
         * input: array of tuples with MessagePack values inside ([t1, t2, ...])
         * <br>
         * converter result: {@code TarantoolResult<TarantoolTuple>}
         * <p>
         * For example, it can be used to parse result from IPROTO_SELECT
         * fields result: some Java type converted from the value of <code>x</code> according to the mapper, passed as a
         * parameter to this one for parsing the result contents.
         *
         * @param messagePackMapper to parse fields
         * @return builder with converter that parses list of arrays to {@code TarantoolResult<TarantoolTuple>}
         */
        Builder withArrayValueToTarantoolTupleResultConverter(
            MessagePackMapper messagePackMapper);

        /**
         * Add converter parses result from a map with list of tuples and metadata.
         * Use this converter for handling containers with {@link TarantoolTuple} inside.
         * The crud method results by default contain map with list of tuples and metadata.
         * This converter can be used at the inner level by other mapper factories that are handling higher-level
         * containers.
         * <p>
         * input: map with metadata and array of tuples with MessagePack values inside ([t1, t2, ...])
         * <br>
         * fields result: some Java type converted from the value of <code>x</code> according to the mapper, passed as a
         * parameter to builder constructor for parsing the result contents.
         * <p>
         *
         * @return builder with converter that parses map to {@code TarantoolResult<TarantoolTuple>}
         */
        Builder withRowsMetadataToTarantoolTupleResultConverter();

        /**
         * Add converter parses result from a map with list of tuples and metadata.
         * Use this converter for handling containers with {@link TarantoolTuple} inside.
         * The crud method results by default contain map with list of tuples and metadata.
         * This converter can be used at the inner level by other mapper factories that are handling higher-level
         * containers.
         * <p>
         * input: map with metadata and array of tuples with MessagePack values inside ([t1, t2, ...])
         * <br>
         * fields result: some Java type converted from the value of <code>x</code> according to the mapper, passed as a
         * parameter to this one for parsing the result contents.
         * <p>
         *
         * @param messagePackMapper to parse fields
         * @return builder with converter that parses map to {@code TarantoolResult<TarantoolTuple>}
         */
        Builder withRowsMetadataToTarantoolTupleResultConverter(
            MessagePackMapper messagePackMapper);

        MessagePackValueMapper buildCallResultMapper();

        MessagePackValueMapper buildCallResultMapper(MessagePackMapper valueMapper);

        <T> CallResultMapper<T, SingleValueCallResult<T>>
        buildSingleValueResultMapper(MessagePackValueMapper valueMapper, Class<T> classResult);
    }
}
