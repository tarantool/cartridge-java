package io.tarantool.driver.proxy;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapperFactory;
import io.tarantool.driver.protocol.operations.TupleOperations;
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

    @Test
    public void deleteOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new DeleteProxyOperation.Builder<>().build());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(42L));

        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolCallResultMapperFactory mapperFactory = new TarantoolCallResultMapperFactory(defaultMapper);
        TarantoolCallResultMapper<TarantoolTuple> resultMapper = mapperFactory.withDefaultTupleValueConverter(null);

        DeleteProxyOperation<TarantoolTuple> deleteProxyOperation = new DeleteProxyOperation.Builder<TarantoolTuple>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withIndexQuery(indexQuery)
                .withResultMapper(resultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, deleteProxyOperation.getClient());
        assertEquals("function1", deleteProxyOperation.getFunctionName());
        assertEquals(Arrays.asList("space1", Collections.singletonList(42L), options),
                deleteProxyOperation.getArguments());
        assertEquals(resultMapper, deleteProxyOperation.getResultMapper());
    }

    @Test
    public void insertOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new InsertProxyOperation.Builder<>().build());

        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolCallResultMapperFactory mapperFactory = new TarantoolCallResultMapperFactory(defaultMapper);
        TarantoolCallResultMapper<TarantoolTuple> resultMapper = mapperFactory.withDefaultTupleValueConverter(null);

        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, defaultMapper);

        InsertProxyOperation<TarantoolTuple> insertOperation = new InsertProxyOperation.Builder<TarantoolTuple>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withResultMapper(resultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, insertOperation.getClient());
        assertEquals("function1", insertOperation.getFunctionName());
        assertEquals(Arrays.asList("space1", tarantoolTuple.getFields(), options), insertOperation.getArguments());
        assertEquals(resultMapper, insertOperation.getResultMapper());
    }

    @Test
    public void replaceOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new ReplaceProxyOperation.Builder<>().build());

        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolCallResultMapperFactory mapperFactory = new TarantoolCallResultMapperFactory(defaultMapper);
        TarantoolCallResultMapper<TarantoolTuple> resultMapper = mapperFactory.withDefaultTupleValueConverter(null);

        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, defaultMapper);

        ReplaceProxyOperation<TarantoolTuple> operation = new ReplaceProxyOperation.Builder<TarantoolTuple>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withResultMapper(resultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertEquals(Arrays.asList("space1", tarantoolTuple.getFields(), options), operation.getArguments());
        assertEquals(resultMapper, operation.getResultMapper());
    }

    @Test
    public void selectOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new SelectProxyOperation.Builder<>().build());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(5L));

        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolCallResultMapperFactory mapperFactory = new TarantoolCallResultMapperFactory(defaultMapper);
        TarantoolCallResultMapper<TarantoolTuple> resultMapper = mapperFactory.withDefaultTupleValueConverter(null);

        List<?> selectArguments = Collections.singletonList(Arrays.asList("=", "id", 55));

        TarantoolSelectOptions selectOptions = new TarantoolSelectOptions.Builder().withLimit(100).build();

        SelectProxyOperation<TarantoolTuple> op = new SelectProxyOperation.Builder<TarantoolTuple>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withSelectArguments(selectArguments)
                .withSelectOptions(selectOptions)
                .withResultMapper(resultMapper)
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
        assertEquals(resultMapper, op.getResultMapper());
    }

    @Test
    public void updateOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new UpdateProxyOperation.Builder<>().build());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(10));

        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolCallResultMapperFactory mapperFactory = new TarantoolCallResultMapperFactory(defaultMapper);
        TarantoolCallResultMapper<TarantoolTuple> resultMapper = mapperFactory.withDefaultTupleValueConverter(null);

        UpdateProxyOperation<TarantoolTuple> operation = new UpdateProxyOperation.Builder<TarantoolTuple>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withIndexQuery(indexQuery)
                .withTupleOperation(TupleOperations.add(3, 90).andAdd(4, 5))
                .withResultMapper(resultMapper)
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

        assertEquals(resultMapper, operation.getResultMapper());
    }

    @Test
    public void upsertOperationBuilderTest() {
        assertThrows(IllegalArgumentException.class, () -> new UpsertProxyOperation.Builder<>().build());

        TarantoolIndexQuery indexQuery = new TarantoolIndexQuery();
        indexQuery.withKeyValues(Collections.singletonList(10));

        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolCallResultMapperFactory mapperFactory = new TarantoolCallResultMapperFactory(defaultMapper);
        TarantoolCallResultMapper<TarantoolTuple> resultMapper = mapperFactory.withDefaultTupleValueConverter(null);

        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, defaultMapper);

        UpsertProxyOperation<TarantoolTuple> operation = new UpsertProxyOperation.Builder<TarantoolTuple>()
                .withClient(client)
                .withSpaceName("space1")
                .withFunctionName("function1")
                .withTuple(tarantoolTuple)
                .withTupleOperation(TupleOperations.add(3, 90).andAdd(4, 5))
                .withResultMapper(resultMapper)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CRUDOperationOptions.TIMEOUT, client.getConfig().getRequestTimeout());

        assertEquals(client, operation.getClient());
        assertEquals("function1", operation.getFunctionName());
        assertEquals(Arrays.asList("space1", tarantoolTuple.getFields(),
                TupleOperations.add(3, 90).andAdd(4, 5).asProxyOperationList(), options),
                operation.getArguments());
        assertEquals(resultMapper, operation.getResultMapper());
    }
}
