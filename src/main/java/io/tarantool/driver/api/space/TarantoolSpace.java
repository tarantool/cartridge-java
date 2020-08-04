package io.tarantool.driver.api.space;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClientException;
import io.tarantool.driver.TarantoolConnection;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolIndexQueryFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.exceptions.TarantoolValueConverterNotFoundException;
import io.tarantool.driver.mappers.TarantoolResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.requests.TarantoolDeleteRequest;
import io.tarantool.driver.protocol.requests.TarantoolInsertRequest;
import io.tarantool.driver.protocol.requests.TarantoolReplaceRequest;
import io.tarantool.driver.protocol.requests.TarantoolSelectRequest;
import org.msgpack.value.ArrayValue;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Basic Tarantool space operations implementation for standalone server
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpace implements TarantoolSpaceOperations {

    private int spaceId;
    private TarantoolClientConfig config;
    private TarantoolConnection connection;
    private RequestFutureManager requestManager;
    private TarantoolIndexQueryFactory indexQueryFactory;
    private TarantoolResultMapperFactory tarantoolResultMapperFactory;

    public TarantoolSpace(int spaceId, TarantoolClientConfig config, TarantoolConnection connection, RequestFutureManager requestManager) {
        this.spaceId = spaceId;
        this.config = config;
        this.connection = connection;
        this.requestManager = requestManager;
        this.indexQueryFactory = new TarantoolIndexQueryFactory(connection);
        this.tarantoolResultMapperFactory = new TarantoolResultMapperFactory();
    }

    /**
     * Get space ID
     * @return space ID on the Tarantool server
     */
    public int getSpaceId() {
        return spaceId;
    }

    /**
     * Get name of the space
     * @return nullable name wrapped in {@code Optional}
     * @throws TarantoolClientException if failed to retrieve the space information from Tarantool server
     */
    public String getName() throws TarantoolClientException {
        Optional<TarantoolSpaceMetadata> meta = connection.metadata().getSpaceById(spaceId);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceId);
        }
        return meta.get().getSpaceName();
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> delete(TarantoolIndexQuery indexQuery)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, TarantoolTuple> converter = getDefaultArrayValueToTupleConverter();
        return delete(indexQuery, converter);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> delete(TarantoolIndexQuery indexQuery, ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        try {
            TarantoolDeleteRequest request = new TarantoolDeleteRequest.Builder()
                    .withSpaceId(spaceId)
                    .withIndexId(indexQuery.getIndexId())
                    .withKeyValues(indexQuery.getKeyValues())
                    .build(config.getObjectMapper());

            return sendRequest(request, tupleMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> insert(TarantoolTuple tuple)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, TarantoolTuple> converter = getDefaultArrayValueToTupleConverter();
        return insert(tuple, converter);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple, ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        try {
            TarantoolInsertRequest request = new TarantoolInsertRequest.Builder()
                    .withSpaceId(spaceId)
                    .withTuple(tuple)
                    .build(config.getObjectMapper());

            return sendRequest(request, tupleMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> replace(TarantoolTuple tuple) throws TarantoolClientException {
        ValueConverter<ArrayValue, TarantoolTuple> converter = getDefaultArrayValueToTupleConverter();
        return replace(tuple, converter);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> replace(TarantoolTuple tuple, ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        try {
            TarantoolReplaceRequest request = new TarantoolReplaceRequest.Builder()
                    .withSpaceId(spaceId)
                    .withTuple(tuple)
                    .build(config.getObjectMapper());

            return sendRequest(request, tupleMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolSelectOptions options)
            throws TarantoolClientException {
        return select(indexQueryFactory.primary(), options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byName(spaceId, indexName);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName,
                                                                     TarantoolIteratorType iteratorType,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byName(spaceId, indexName).withIteratorType(iteratorType);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolIndexQuery indexQuery,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, TarantoolTuple> converter = getDefaultArrayValueToTupleConverter();
        return select(indexQuery, options, converter);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                            TarantoolSelectOptions options,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        try {
            TarantoolSelectRequest request = new TarantoolSelectRequest.Builder()
                    .withSpaceId(spaceId)
                    .withIndexId(indexQuery.getIndexId())
                    .withIteratorType(indexQuery.getIteratorType())
                    .withKeyValues(indexQuery.getKeyValues())
                    .withLimit(options.getLimit())
                    .withOffset(options.getOffset())
                    .build(config.getObjectMapper());

            return sendRequest(request, tupleMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    private <T> CompletableFuture<TarantoolResult<T>> sendRequest(TarantoolRequest request, ValueConverter<ArrayValue, T> tupleMapper) {
        CompletableFuture<TarantoolResult<T>> requestFuture = requestManager.submitRequest(
                request, tarantoolResultMapperFactory.withConverter(tupleMapper));

        connection.getChannel().writeAndFlush(request).addListener(f -> {
            if (!f.isSuccess()) {
                requestFuture.completeExceptionally(
                        new RuntimeException("Failed to send the request to Tarantool server", f.cause()));
            }
        });

        return requestFuture;
    }

    private ValueConverter<ArrayValue, TarantoolTuple> getDefaultArrayValueToTupleConverter() {
        Optional<ValueConverter<ArrayValue, TarantoolTuple>> converter =
                config.getValueMapper().getValueConverter(ArrayValue.class, TarantoolTuple.class);
        if (!converter.isPresent()) {
            throw new TarantoolValueConverterNotFoundException(ArrayValue.class, TarantoolTuple.class);
        }
        return converter.get();
    }

    @Override
    public String toString() {
        return String.format("TarantoolSpace [%d]", spaceId);
    }
}
