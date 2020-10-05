package io.tarantool.driver.api;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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
    CompletableFuture<List<Object>> eval(String expression) throws TarantoolClientException;

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
    CompletableFuture<List<Object>> eval(String expression, List<Object> arguments) throws TarantoolClientException;

    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>.
     *
     * @param expression   lua expression, must not be null or empty
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException;

    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>.
     *
     * @param expression   lua expression, must not be null or empty
     * @param arguments    the list of function arguments. The object mapper specified in the client configuration
     *                     will be used for arguments conversion to MessagePack entities
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> eval(String expression,
                                         List<Object> arguments,
                                         MessagePackValueMapper resultMapper)
            throws TarantoolClientException;

    /**
     * Execute a Lua expression in the Tarantool instance. If a result is expected, the expression must start with
     * keyword <code>return</code>.
     *
     * @param expression      lua expression, must not be null or empty
     * @param arguments       the list of function arguments
     * @param argumentsMapper mapper for arguments object-to-MessagePack entity conversion
     * @param resultMapper    mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> eval(String expression,
                                         List<Object> arguments,
                                         MessagePackObjectMapper argumentsMapper,
                                         MessagePackValueMapper resultMapper) throws TarantoolClientException;
}
