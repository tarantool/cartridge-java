package io.tarantool.driver;

import io.netty.channel.Channel;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TarantoolConnection extends AutoCloseable {
    /**
     * Get the Tarantool server version
     * @return {@link TarantoolVersion}
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolVersion getVersion() throws TarantoolClientException;

    /**
     * Provides CRUD and other operations for a Tarantool space
     * @param spaceName name of the space, must not be null or empty
     * @return Tarantool space operations interface
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolSpaceOperations space(String spaceName) throws TarantoolClientException;

    /**
     * Provides CRUD and other operations for a Tarantool space
     * @param spaceId ID of the space, must be greater than 0
     * @return Tarantool space operations implementation
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException;

    /**
     * Provides operations for Tarantool spaces and indexes metadata
     * @return Tarantool metadata operations implementation
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolMetadataOperations metadata() throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * TODO example function call
     * @param functionName function name, must not be null or empty
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> call(String functionName) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * @param functionName function name, must not be null or empty
     * @param arguments the list of function arguments. The object mapper specified in the client configuration
     *                  will be used for arguments conversion to MessagePack entities
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> call(String functionName, List<Object> arguments) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * @param functionName function name, must not be null or empty
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> call(String functionName, MessagePackValueMapper resultMapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * @param <T> the desired function call result type. The object mapper specified in the client configuration
     * will be used for result value conversion
     * @param functionName function name, must not be null or empty
     * @param arguments the list of function arguments. The object mapper specified in the client configuration
     *        will be used for arguments conversion to MessagePack entities
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<List<T>> call(String functionName, List<Object> arguments, MessagePackValueMapper resultMapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * @param functionName function name, must not be null or empty
     * @param arguments the list of function arguments. The object mapper specified in the client configuration
     *        will be used for arguments conversion to MessagePack entities
     * @param argumentsMapper mapper for arguments object-to-MessagePack entity conversion
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @param <T> the desired function call result type. The object mapper specified in the client configuration
     * will be used for result value conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<List<T>> call(String functionName, List<Object> arguments, MessagePackObjectMapper argumentsMapper,
                                  MessagePackValueMapper resultMapper)
            throws TarantoolClientException;

    /**
     * Eval lua expression on Tarantool instance
     * @param expression lua expression, must not be null or empty
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> eval(String expression) throws TarantoolClientException;

    /**
     * Eval lua expression on Tarantool instance
     * @param expression lua expression, must not be null or empty
     * @param arguments the list of function arguments. The object mapper specified in the client configuration
     *                  will be used for arguments conversion to MessagePack entities
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    CompletableFuture<List<Object>> eval(String expression, List<Object> arguments) throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * @param <T> the desired function call result type. The object mapper specified in the client configuration
     * will be used for result value conversion
     * @param expression lua expression, must not be null or empty
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<List<T>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * @param <T> the desired function call result type. The object mapper specified in the client configuration
     * will be used for result value conversion
     * @param expression lua expression, must not be null or empty
     * @param arguments the list of function arguments. The object mapper specified in the client configuration
     *        will be used for arguments conversion to MessagePack entities
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<List<T>> eval(String expression, List<Object> arguments, MessagePackValueMapper resultMapper)
            throws TarantoolClientException;

    /**
     * Execute a function defined on Tarantool instance
     * @param expression lua expression, must not be null or empty
     * @param arguments the list of function arguments. The object mapper specified in the client configuration
     *        will be used for arguments conversion to MessagePack entities
     * @param argumentsMapper mapper for arguments object-to-MessagePack entity conversion
     * @param resultMapper mapper for result value MessagePack entity-to-object conversion
     * @param <T> the desired function call result type. The object mapper specified in the client configuration
     * will be used for result value conversion
     * @return some result
     * @throws TarantoolClientException if the client is not connected
     */
    <T> CompletableFuture<List<T>> eval(String expression, List<Object> arguments,
                                        MessagePackObjectMapper argumentsMapper,
                                        MessagePackValueMapper resultMapper) throws TarantoolClientException;

    /**
     * Get the connection closed status
     * @return true, if the connection is closed
     */
    boolean isClosed();

    /**
     * Get the Netty channel baking up this connection
     * @return channel
     */
    Channel getChannel();
}
