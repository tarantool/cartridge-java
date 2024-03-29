package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Aggregates all value operation variants
 *
 * @author Alexey Kuzin
 */
public interface TarantoolEvalOperations {
    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>. The value mapper specified in the client configuration will be used for converting
     * the result values from MessagePack entities to objects.
     *
     * @param expression lua expression, must not be null or empty
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> eval(String expression) throws TarantoolClientException;

    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>. The value mapper specified in the client configuration will be used for converting
     * the result values from MessagePack entities to objects.
     *
     * @param expression lua expression, must not be null or empty
     * @param arguments  the list of function arguments. The object mapper specified in the client configuration
     *                   will be used for arguments conversion to MessagePack entities
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> eval(String expression, Collection<?> arguments) throws TarantoolClientException;

    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>.
     *
     * @param expression   lua expression, must not be null or empty
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> eval(String expression, Supplier<? extends MessagePackValueMapper> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>.
     *
     * @param expression   lua expression, must not be null or empty
     * @param arguments    the list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @param resultMapperSupplier mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> eval(
        String expression, Collection<?> arguments, Supplier<? extends MessagePackValueMapper> resultMapperSupplier)
        throws TarantoolClientException;

    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>.
     *
     * @param expression      lua expression, must not be null or empty
     * @param arguments       the list of function arguments
     * @param argumentsMapperSupplier mapper supplier for arguments object-to-MessagePack entity conversion
     * @param resultMapperSupplier    mapper supplier for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<?>> eval(
        String expression,
        Collection<?> arguments,
        Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<? extends MessagePackValueMapper> resultMapperSupplier) throws TarantoolClientException;
}
