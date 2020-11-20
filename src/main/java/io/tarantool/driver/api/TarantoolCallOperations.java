package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Aggregates all call operation variants
 *
 * @author Alexey Kuzin
 */
public interface TarantoolCallOperations {
    /**
     * Execute a function defined on Tarantool instance. The value mapper specified in the client configuration will be
     * used for converting the result values from MessagePack entities to objects.
     * TODO example function call
     *
     * @param functionName function name, must not be null or empty
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> call(String functionName) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance, The value mapper specified in the client configuration will be
     * used for converting the result values from MessagePack entities to objects.
     *
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> call(String functionName, List<?> arguments) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     *
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param mapper          mapper for arguments object-to-MessagePack entity conversion and result values conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> call(String functionName, List<?> arguments, MessagePackMapper mapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The value mapper specified in the client configuration will be
     * used for converting the result values from MessagePack arrays to the specified entity type.
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param entityClass  target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<TarantoolResult<T>> call(String functionName, Class<T> entityClass)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param tupleMapper  mapper for result value MessagePack array-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<TarantoolResult<T>> call(String functionName, ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The value mapper specified in the client configuration will be
     * used for converting the result values from MessagePack arrays to the specified entity type.
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @param entityClass  target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                   List<?> arguments,
                                                   Class<T> entityClass)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @param tupleMapper  mapper for result value MessagePack array-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                   List<?> arguments,
                                                   ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The value mapper specified in the client configuration will be
     * used for converting the result values from MessagePack arrays to the specified entity type.
     *
     * @param <T>             desired function call result type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapper mapper for arguments object-to-MessagePack entity conversion
     * @param entityClass     target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                   List<?> arguments,
                                                   MessagePackObjectMapper argumentsMapper,
                                                   Class<T> entityClass)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     *
     * @param <T>             desired function call result type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapper mapper for arguments object-to-MessagePack entity conversion
     * @param tupleMapper     mapper for result value MessagePack array-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                   List<?> arguments,
                                                   MessagePackObjectMapper argumentsMapper,
                                                   ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     *
     * @param <T>             desired function call result type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapper mapper for arguments object-to-MessagePack entity conversion
     * @param resultMapper    mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                   List<?> arguments,
                                                   MessagePackObjectMapper argumentsMapper,
                                                   TarantoolCallResultMapper<T> resultMapper)
            throws TarantoolClientException;
}
