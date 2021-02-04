package io.tarantool.driver.proxy;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.DefaultResultMapperFactoryFactory;
import io.tarantool.driver.metadata.TarantoolMetadata;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.metadata.TestMetadataProvider;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProxyOperationBuildersTest {

    private static final TarantoolClient client = new StandaloneTarantoolClient();
    private final MessagePackMapper defaultMapper =
            DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private final DefaultResultMapperFactoryFactory mapperFactoryFactory =
            new DefaultResultMapperFactoryFactory();
    private final
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    defaultResultMapper = mapperFactoryFactory.defaultTupleSingleResultMapperFactory()
            .withDefaultTupleValueConverter(defaultMapper, null);

    @Test
    public void deleteOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new DeleteProxyOperation.Builder<>().build());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(42L));

        DeleteProxyOperation<TarantoolResult<TarantoolTuple>> deleteProxyOperation =
                new DeleteProxyOperation.Builder<TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withIndexQuery(indexQuery)
                .withResultMapper(defaultResultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, deleteProxyOperation.getClient());
        assertEquals("function1", deleteProxyOperation.getFunctionName());
        assertEquals(Arrays.asList("space1", Collections.singletonList(42L), options),
                deleteProxyOperation.getArguments());
        assertEquals(defaultResultMapper, deleteProxyOperation.getResultMapper());
    }

    @Test
    public void insertOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new InsertProxyOperation.Builder<>().build());

        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, defaultMapper);

        InsertProxyOperation<TarantoolResult<TarantoolTuple>> insertOperation =
                new InsertProxyOperation.Builder<TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withResultMapper(defaultResultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, insertOperation.getClient());
        assertEquals("function1", insertOperation.getFunctionName());
        assertEquals(Arrays.asList("space1", tarantoolTuple.getFields(), options), insertOperation.getArguments());
        assertEquals(defaultResultMapper, insertOperation.getResultMapper());
    }

    @Test
    public void replaceOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new ReplaceProxyOperation.Builder<>().build());

        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, defaultMapper);

        ReplaceProxyOperation<TarantoolResult<TarantoolTuple>> operation =
                new ReplaceProxyOperation.Builder<TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withResultMapper(defaultResultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertEquals(Arrays.asList("space1", tarantoolTuple.getFields(), options), operation.getArguments());
        assertEquals(defaultResultMapper, operation.getResultMapper());
    }

    @Test
    public void selectOperationBuilderTest() {
        TarantoolMetadata testOperations = new TarantoolMetadata(new TestMetadataProvider());
        assertThrows(IllegalArgumentException.class, () ->
                new SelectProxyOperation.Builder<>(
                        testOperations, testOperations.getSpaceByName("test").get()).build());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(5L));

        List<?> selectArguments = Collections.singletonList(Arrays.asList("=", "second", 55));
        Conditions conditions = Conditions.equals("second", 55).withLimit(100);

        SelectProxyOperation<TarantoolResult<TarantoolTuple>> op =
                new SelectProxyOperation.Builder<TarantoolResult<TarantoolTuple>>(
                        testOperations, testOperations.getSpaceByName("test").get())
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withConditions(conditions)
                .withResultMapper(defaultResultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());
        options.put(CRUDOperationOptions.SELECT_BATCH_SIZE, 100L);
        options.put(CRUDOperationOptions.SELECT_LIMIT, 100L);

        assertEquals(client, op.getClient());
        assertEquals("function1", op.getFunctionName());
        assertEquals(3, op.getArguments().size());
        assertEquals("space1", op.getArguments().get(0));
        assertEquals(selectArguments, op.getArguments().get(1));
        assertEquals(options, op.getArguments().get(2));
        assertEquals(defaultResultMapper, op.getResultMapper());
    }

    @Test
    public void updateOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new UpdateProxyOperation.Builder<>().build());

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
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertEquals(4, operation.getArguments().size());

        assertEquals(Arrays.asList("space1",
                Collections.singletonList(10),
                TupleOperations.add(3, 90).andAdd(4, 5).asProxyOperationList(), options),
                operation.getArguments());

        assertEquals(defaultResultMapper, operation.getResultMapper());
    }

    @Test
    public void upsertOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new UpsertProxyOperation.Builder<>().build());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(10));

        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, defaultMapper);

        UpsertProxyOperation<TarantoolResult<TarantoolTuple>> operation =
                new UpsertProxyOperation.Builder<TarantoolResult<TarantoolTuple>>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withTupleOperation(TupleOperations.add(3, 90).andAdd(4, 5))
                .withResultMapper(defaultResultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertEquals(Arrays.asList("space1", tarantoolTuple.getFields(),
                TupleOperations.add(3, 90).andAdd(4, 5).asProxyOperationList(), options),
                operation.getArguments());
        assertEquals(defaultResultMapper, operation.getResultMapper());
    }
}
