package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactory;
import org.msgpack.value.Value;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    CompletableFuture<List<?>> call(String functionName) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance, The value mapper specified in the client configuration will be
     * used for converting the result values from MessagePack entities to objects.
     *
     * @param functionName function name, must not be null or empty
     * @param arguments    vararg array of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    CompletableFuture<List<?>> call(String functionName, Object... arguments) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The value mapper specified in the client configuration will be
     * used for converting the result values from MessagePack entities to objects.
     *
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    CompletableFuture<List<?>> call(String functionName, Collection<?> arguments) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The call result is interpreted as an array of tuples. The value
     * mapper specified in the client configuration will be used for converting the result values from MessagePack
     * arrays to the specified entity type.
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param entityClass  target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<TarantoolResult<T>> callForTupleResult(String functionName, Class<T> entityClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The call result is interpreted as an array of tuples.
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> call(
        String functionName,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The call result is interpreted as an array of tuples.
     * The value mapper specified in the client configuration will be used for converting the result values from
     * MessagePack arrays to the specified entity type.
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @param entityClass  target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<TarantoolResult<T>> callForTupleResult(
        String functionName,
        Collection<?> arguments,
        Class<T> entityClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The call result is interpreted as an array of tuples.
     *
     * @param <T>          desired function call result type
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> call(
        String functionName,
        Collection<?> arguments,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The call result is interpreted as an array of tuples. The value
     * mapper specified in the client configuration will be used for converting the result values from MessagePack
     * arrays to the specified entity type.
     *
     * @param <T>             desired function call result type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param entityClass     target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<TarantoolResult<T>> callForTupleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Class<T> entityClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The call result is interpreted as an array of tuples.
     *
     * @param <T>             desired function call result type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param resultMapperSupplier    mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> call(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>             target result content type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param resultClass     target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Class<T> resultClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>             target result content type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param valueConverter  MessagePack value to entity converter for each result item
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>             target result content type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param resultMapperSupplier    mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>          target result content type
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments
     * @param resultClass  target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Class<T> resultClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>            target result content type
     * @param functionName   function name, must not be null or empty
     * @param arguments      list of function arguments
     * @param valueConverter MessagePack value to entity converter for each result item
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>          target result content type
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>          target result content type
     * @param functionName function name, must not be null or empty
     * @param resultClass  target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Class<T> resultClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>            target result content type
     * @param functionName   function name, must not be null or empty
     * @param valueConverter MessagePack value to entity converter for each result item
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. The first item of the call result will be interpreted as a
     * value, and the second -- as an error
     *
     * @param <T>          target result content type
     * @param functionName function name, must not be null or empty
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T> CompletableFuture<T> callForSingleResult(
        String functionName,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>                     target result content type
     * @param <R>                     target result type
     * @param functionName            function name, must not be null or empty
     * @param arguments               list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param resultContainerSupplier supplier function for new empty result collection
     * @param resultClass             target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<R> resultContainerSupplier,
        Class<T> resultClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>                     target result content type
     * @param <R>                     target result type
     * @param functionName            function name, must not be null or empty
     * @param arguments               list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param resultContainerSupplier supplier function for new empty result collection
     * @param valueConverter          MessagePack value to entity converter for each result item
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<R> resultContainerSupplier,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>             target result content type
     * @param <R>             target result type
     * @param functionName    function name, must not be null or empty
     * @param arguments       list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param resultMapperSupplier    mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>                     target result content type
     * @param <R>                     target result type
     * @param functionName            function name, must not be null or empty
     * @param arguments               list of function arguments
     * @param resultContainerSupplier supplier function for new empty result collection
     * @param resultClass             target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<R> resultContainerSupplier,
        Class<T> resultClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>                     target result content type
     * @param <R>                     target result type
     * @param functionName            function name, must not be null or empty
     * @param arguments               list of function arguments
     * @param resultContainerSupplier supplier function for new empty result collection
     * @param valueConverter          MessagePack value to entity converter for each result item
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<R> resultContainerSupplier,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>          target result content type
     * @param <R>          target result type
     * @param functionName function name, must not be null or empty
     * @param arguments    list of function arguments
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>                     target result content type
     * @param <R>                     target result type
     * @param functionName            function name, must not be null or empty
     * @param resultContainerSupplier supplier function for new empty result collection
     * @param resultClass             target result entity class
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Supplier<R> resultContainerSupplier,
        Class<T> resultClass)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>                     target result content type
     * @param <R>                     target result type
     * @param functionName            function name, must not be null or empty
     * @param resultContainerSupplier supplier function for new empty result collection
     * @param valueConverter          MessagePack value to entity converter for each result item
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Supplier<R> resultContainerSupplier,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance. All multi-return result items will be put into a list
     *
     * @param <T>          target result content type
     * @param <R>          target result type
     * @param functionName function name, must not be null or empty
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected or some other error occurred
     */
    <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Get the default factory for result mapper factory instances
     *
     * @return result mapper factory instances factory instance
     */
    ResultMapperFactoryFactory getResultMapperFactoryFactory();
}
