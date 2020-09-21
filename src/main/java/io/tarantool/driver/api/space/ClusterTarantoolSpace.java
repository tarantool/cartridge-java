package io.tarantool.driver.api.space;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolIndexQueryFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.cluster.ClusterOperationOptions;
import io.tarantool.driver.cluster.ClusterOperationsMappingConfig;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.TarantoolResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.operations.TupleOperations;
import org.msgpack.value.ArrayValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author Sergey Volgin
 */
public class ClusterTarantoolSpace implements TarantoolSpaceOperations {

    private final String spaceName;
    private final TarantoolClient client;
    private final TarantoolClientConfig config;
    private final TarantoolIndexQueryFactory indexQueryFactory;
    private final ClusterOperationsMappingConfig mapping;
    private final TarantoolMetadataOperations operations;

    private final TarantoolResultMapperFactory tarantoolResultMapperFactory;

    public ClusterTarantoolSpace(TarantoolClient client,
                                 String spaceName,
                                 TarantoolMetadataOperations operations) {
        this.spaceName = spaceName;
        this.client = client;
        this.config = client.getConfig();
        this.tarantoolResultMapperFactory = new TarantoolResultMapperFactory();
        tarantoolResultMapperFactory.withConverter(getDefaultTarantoolTupleValueConverter());
        this.operations = operations;
        this.indexQueryFactory = new TarantoolIndexQueryFactory(operations);
        this.mapping = config.getClusterOperationsMappingConfig();
        if (mapping == null) {
            throw new TarantoolClientException("Not found proxy operation mapping for space %s", spaceName);
        }
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> delete(TarantoolIndexQuery indexQuery)
            throws TarantoolClientException {
        return delete(indexQuery, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> delete(TarantoolIndexQuery indexQuery,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        ClusterOperationOptions options = ClusterOperationOptions.builder()
                .withTimeout(config.getRequestTimeout())
                .withTuplesAsMap(false)
                .build();

        return sendRequest(mapping.getDeleteFunctionName(),
                Arrays.asList(spaceName, indexQuery.getKeyValues(), options.asMap()),
                tupleMapper);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> insert(TarantoolTuple tuple)
            throws TarantoolClientException {
        return insert(tuple, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        ClusterOperationOptions options = ClusterOperationOptions.builder()
                .withTimeout(config.getRequestTimeout())
                .withTuplesAsMap(false)
                .build();

        return sendRequest(mapping.getInsertFunctionName(),
                Arrays.asList(spaceName, getIndexKeyParts(TarantoolIndexQuery.PRIMARY),
                        tuple.getFields(), options.asMap()),
                tupleMapper);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> replace(TarantoolTuple tuple)
            throws TarantoolClientException {
        return replace(tuple, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> replace(TarantoolTuple tuple,
                                                             ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        ClusterOperationOptions options = ClusterOperationOptions.builder()
                .withTimeout(config.getRequestTimeout())
                .withTuplesAsMap(false)
                .build();

        return sendRequest(mapping.getReplaceFunctionName(),
                Arrays.asList(spaceName, getIndexKeyParts(TarantoolIndexQuery.PRIMARY),
                        tuple.getFields(), options.asMap()),
                tupleMapper);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolSelectOptions options)
            throws TarantoolClientException {
        return select(indexQueryFactory.primary(), options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName, TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byId(indexName, spaceName, operations);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName,
                                                                     TarantoolIteratorType iteratorType,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byId(indexName, spaceName, operations)
                .withIteratorType(iteratorType);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolIndexQuery indexQuery,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, TarantoolTuple> converter = getDefaultTarantoolTupleValueConverter();
        return select(indexQuery, options, converter);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                     TarantoolSelectOptions options,
                                                     Class<T> clazz) throws TarantoolClientException {
        Optional<ValueConverter<ArrayValue, T>> tupleMapper = tarantoolResultMapperFactory.getValueConverter(clazz);
        if (!tupleMapper.isPresent()) {
            throw new TarantoolClientException("Converter for class %s not found", clazz);
        }
        return select(indexQuery, options, tupleMapper.get());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                            TarantoolSelectOptions options,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {

        ClusterOperationOptions requestOptions = ClusterOperationOptions.builder()
                .withTimeout(config.getRequestTimeout())
                .withTuplesAsMap(false)
                .withSelectKey(indexQuery.getKeyValues())
                .withSelectIterator(indexQuery.getIteratorType().getStringCode())
                .withSelectOffset(options.getOffset())
                .withSelectBatchSize(options.getLimit())
                .withSelectLimit(options.getLimit())
                .build();

        return sendRequest(mapping.getSelectFunctionName(),
                Arrays.asList(spaceName, getIndexKeyParts(indexQuery.getIndexId()), requestOptions.asMap()),
                tupleMapper);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> update(TarantoolIndexQuery indexQuery,
                                                                     TupleOperations operations) {
        return update(indexQuery, operations, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> update(TarantoolIndexQuery indexQuery,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper) {
        ClusterOperationOptions options = ClusterOperationOptions.builder()
                .withTimeout(config.getRequestTimeout())
                .withTuplesAsMap(false)
                .build();

        return sendRequest(mapping.getUpdateFunctionName(),
                Arrays.asList(spaceName, indexQuery.getKeyValues(),
                        operations.asListByPositionNumber(), options.asMap()),
                tupleMapper);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> upsert(TarantoolIndexQuery indexQuery,
                                                                     TarantoolTuple tuple,
                                                                     TupleOperations operations) {
        return upsert(indexQuery, tuple, operations, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> upsert(TarantoolIndexQuery indexQuery,
                                                            TarantoolTuple tuple,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper) {
        ClusterOperationOptions options = ClusterOperationOptions.builder()
                .withTimeout(config.getRequestTimeout())
                .withTuplesAsMap(false)
                .build();

        return sendRequest(mapping.getUpsertFunctionName(),
                Arrays.asList(spaceName, getIndexKeyParts(TarantoolIndexQuery.PRIMARY),
                        tuple, operations.asListByPositionNumber(), options.asMap()),
                tupleMapper);
    }

    private <T> CompletableFuture<TarantoolResult<T>> sendRequest(String functionName,
                                                                  List<Object> arguments,
                                                                  ValueConverter<ArrayValue, T> tupleMapper) {
        MessagePackMapper argumentMapper = config.getMessagePackMapper();
        return client.call(functionName,
                arguments,
                argumentMapper,
                tupleMapper
        );
    }

    private ValueConverter<ArrayValue, TarantoolTuple> getDefaultTarantoolTupleValueConverter() {
        return tarantoolResultMapperFactory.getDefaultTupleValueConverter(config.getMessagePackMapper());
    }

    private List<Integer> getIndexKeyParts(int indexId) {
        Optional<TarantoolIndexMetadata> indexMetadata = operations.getIndexById(spaceName, indexId);
        List<Integer> keyParts = new ArrayList<>();

        if (indexMetadata.isPresent()) {
            keyParts = indexMetadata.get().getIndexParts().stream()
                    .map(p -> p.getFieldIndex() + 1).collect(Collectors.toList());
        }

        return keyParts;
    }

    @Override
    public String toString() {
        return String.format("TarantoolSpace [%s]", spaceName);
    }
}
