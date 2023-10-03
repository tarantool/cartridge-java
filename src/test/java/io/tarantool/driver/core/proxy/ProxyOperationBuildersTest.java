package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.options.crud.enums.Mode;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;
import io.tarantool.driver.api.space.options.crud.enums.RollbackOnError;
import io.tarantool.driver.api.space.options.crud.enums.StopOnError;
import io.tarantool.driver.api.space.options.ProxyDeleteOptions;
import io.tarantool.driver.api.space.options.ProxyInsertManyOptions;
import io.tarantool.driver.api.space.options.ProxyInsertOptions;
import io.tarantool.driver.api.space.options.ProxyReplaceManyOptions;
import io.tarantool.driver.api.space.options.ProxyReplaceOptions;
import io.tarantool.driver.api.space.options.ProxySelectOptions;
import io.tarantool.driver.api.space.options.ProxyTruncateOptions;
import io.tarantool.driver.api.space.options.ProxyUpdateOptions;
import io.tarantool.driver.api.space.options.ProxyUpsertOptions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.metadata.TarantoolMetadata;
import io.tarantool.driver.core.metadata.TestMetadataProvider;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactory;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactoryImpl;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;


public class ProxyOperationBuildersTest {

    private static final ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient();
    private final MessagePackMapper defaultMapper =
        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private final TarantoolTupleFactory factory = new DefaultTarantoolTupleFactory(defaultMapper);
    TarantoolTupleResultMapperFactory tarantoolTupleResultMapperFactory =
        TarantoolTupleResultMapperFactoryImpl.getInstance();
    private final
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
        defaultResultMapper = tarantoolTupleResultMapperFactory
        .withSingleValueArrayToTarantoolTupleResultMapper(defaultMapper, null);

    @Test
    public void deleteOperationBuilderTest() {
        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(42L));

        DeleteProxyOperation<TarantoolResult<TarantoolTuple>> deleteProxyOperation =
            new DeleteProxyOperation.Builder<TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withIndexQuery(indexQuery)
                .withResultMapper(defaultResultMapper)
                .withArgumentsMapper(defaultMapper)
                .withOptions(ProxyDeleteOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                )
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());

        assertEquals(client, deleteProxyOperation.getClient());
        assertEquals("function1", deleteProxyOperation.getFunctionName());
        assertIterableEquals(Arrays.asList("space1", Collections.singletonList(42L), options),
            deleteProxyOperation.getArguments());
        assertEquals(defaultResultMapper, deleteProxyOperation.getResultMapper());
    }

    @Test
    public void insertOperationBuilderTest() {
        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = factory.create(values);

        InsertProxyOperation<TarantoolTuple, TarantoolResult<TarantoolTuple>> insertOperation =
            new InsertProxyOperation.Builder<TarantoolTuple, TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withArgumentsMapper(defaultMapper)
                .withResultMapper(defaultResultMapper)
                .withOptions(ProxyInsertOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                )
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());

        assertEquals(client, insertOperation.getClient());
        assertEquals("function1", insertOperation.getFunctionName());
        assertIterableEquals(Arrays.asList("space1", tarantoolTuple, options), insertOperation.getArguments());
        assertEquals(defaultResultMapper, insertOperation.getResultMapper());
    }

    @Test
    public void insertManyOperationBuilderTest() {
        List<TarantoolTuple> tarantoolTuples = Arrays.asList(
            factory.create(Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984)),
            factory.create(Arrays.asList(44, "a44", "Silmarillion", "J. R. R. Tolkien", 1977))
        );

        InsertManyProxyOperation<TarantoolTuple, TarantoolResult<TarantoolTuple>> operation =
            new InsertManyProxyOperation.Builder<TarantoolTuple, TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuples(tarantoolTuples)
                .withResultMapper(defaultResultMapper)
                .withArgumentsMapper(defaultMapper)
                .withOptions(ProxyInsertManyOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                    .withRollbackOnError(RollbackOnError.TRUE)
                    .withStopOnError(StopOnError.FALSE)
                )
                .build();
        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());
        options.put(ProxyOption.ROLLBACK_ON_ERROR.toString(), true);
        options.put(ProxyOption.STOP_ON_ERROR.toString(), false);

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertIterableEquals(Arrays.asList("space1", tarantoolTuples, options), operation.getArguments());
        assertEquals(defaultResultMapper, operation.getResultMapper());
    }

    @Test
    public void replaceOperationBuilderTest() {
        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = factory.create(values);

        ReplaceProxyOperation<TarantoolTuple, TarantoolResult<TarantoolTuple>> operation =
            new ReplaceProxyOperation.Builder<TarantoolTuple, TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withResultMapper(defaultResultMapper)
                .withArgumentsMapper(defaultMapper)
                .withOptions(ProxyReplaceOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                )
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertIterableEquals(Arrays.asList("space1", tarantoolTuple, options), operation.getArguments());
        assertEquals(defaultResultMapper, operation.getResultMapper());
    }

    @Test
    public void replaceManyOperationBuilderTest() {
        List<TarantoolTuple> tarantoolTuples = Arrays.asList(
            factory.create(Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984)),
            factory.create(Arrays.asList(44, "a44", "Silmarillion", "J. R. R. Tolkien", 1977))
        );

        ReplaceManyProxyOperation<TarantoolTuple, TarantoolResult<TarantoolTuple>> operation =
            new ReplaceManyProxyOperation.Builder<TarantoolTuple, TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuples(tarantoolTuples)
                .withResultMapper(defaultResultMapper)
                .withArgumentsMapper(defaultMapper)
                .withOptions(ProxyReplaceManyOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                    .withRollbackOnError(RollbackOnError.TRUE)
                    .withStopOnError(StopOnError.FALSE)
                )
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());
        options.put(ProxyOption.ROLLBACK_ON_ERROR.toString(), true);
        options.put(ProxyOption.STOP_ON_ERROR.toString(), false);

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertIterableEquals(Arrays.asList("space1", tarantoolTuples, options), operation.getArguments());
        assertEquals(defaultResultMapper, operation.getResultMapper());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectOperationBuilderTest() {
        TarantoolMetadata testOperations = new TarantoolMetadata(new TestMetadataProvider());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(5L));

        List<?> selectArguments = Collections.singletonList(Arrays.asList("=", "second", 55));
        Conditions conditions = Conditions.equals("second", 55).withLimit(100);

        SelectProxyOperation<TarantoolResult<TarantoolTuple>> op =
            new SelectProxyOperation.Builder<TarantoolResult<TarantoolTuple>>(
                testOperations, testOperations.getSpaceByName("test").get())
                .withClient(client)
                .withSpaceName("space1")
                .withConditions(conditions)
                .withFunctionName("function1")
                .withResultMapper(defaultResultMapper)
                .withArgumentsMapper(defaultMapper)
                .withOptions(ProxySelectOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                    .withBatchSize(123456)
                    .withMode(Mode.WRITE)
                )
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());
        options.put(ProxyOption.BATCH_SIZE.toString(), 123456);
        options.put(ProxyOption.FIRST.toString(), 100L);
        options.put(ProxyOption.MODE.toString(), Mode.WRITE.value());

        assertEquals(client, op.getClient());
        assertEquals("function1", op.getFunctionName());
        assertEquals(3, op.getArguments().size());
        List<Object> argumentValues = new ArrayList<>(op.getArguments());
        assertEquals("space1", argumentValues.get(0));
        assertEquals(selectArguments, argumentValues.get(1));
        Map<String, Object> actualOptions = (Map<String, Object>) argumentValues.get(2);
        assertEquals(options.toString(), actualOptions.toString());
        assertEquals(defaultResultMapper, op.getResultMapper());
    }

    @Test
    public void updateOperationBuilderTest() {
        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(10));

        UpdateProxyOperation<TarantoolResult<TarantoolTuple>> operation =
            new UpdateProxyOperation.Builder<TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withIndexQuery(indexQuery)
                .withTupleOperation(TupleOperations.add(3, 90).andAdd(4, 5))
                .withResultMapper(defaultResultMapper)
                .withArgumentsMapper(defaultMapper)
                .withOptions(ProxyUpdateOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                )
                .build();
        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertEquals(4, operation.getArguments().size());

        assertIterableEquals(Arrays.asList("space1",
                Collections.singletonList(10),
                TupleOperations.add(3, 90).andAdd(4, 5).asProxyOperationList(), options),
            operation.getArguments());

        assertEquals(defaultResultMapper, operation.getResultMapper());
    }

    @Test
    public void upsertOperationBuilderTest() {
        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(10));

        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = factory.create(values);

        UpsertProxyOperation<TarantoolTuple, TarantoolResult<TarantoolTuple>> operation =
            new UpsertProxyOperation.Builder<TarantoolTuple, TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withTupleOperation(TupleOperations.add(3, 90).andAdd(4, 5))
                .withResultMapper(defaultResultMapper)
                .withArgumentsMapper(defaultMapper)
                .withOptions(ProxyUpsertOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                )
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertIterableEquals(Arrays.asList("space1", tarantoolTuple,
                TupleOperations.add(3, 90).andAdd(4, 5).asProxyOperationList(), options),
            operation.getArguments());
        assertEquals(defaultResultMapper, operation.getResultMapper());
    }

    @Test
    public void test_truncateOperationBuilder_shouldReturnTruncateOperationObjectsWithAllProperties() {
        // build truncateOperation
        TruncateProxyOperation truncateOperation =
            TruncateProxyOperation.builder()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withOptions(ProxyTruncateOptions.create()
                    .withTimeout(client.getConfig().getRequestTimeout())
                )
                .build();

        // when prepare Map with options
        Map<String, Object> options = new HashMap<>();
        options.put(ProxyOption.TIMEOUT.toString(), client.getConfig().getRequestTimeout());

        // then data is prepared check that is equals that we submitted to the builder
        assertEquals(client, truncateOperation.getClient());
        assertEquals("function1", truncateOperation.getFunctionName());
        assertIterableEquals(Arrays.asList("space1", options), truncateOperation.getArguments());
    }
}
