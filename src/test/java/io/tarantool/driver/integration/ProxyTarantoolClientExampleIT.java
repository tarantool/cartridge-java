package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.options.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Ivan Dneprov
 * <p>
 * WARNING: If you updated the code in this file, don't forget to update the docs/ProxyTarantoolClient.md
 * and docs/TarantoolTupleUsage.md permalinks!
 */
public class ProxyTarantoolClientExampleIT extends SharedCartridgeContainer {

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    public static String USER_NAME;
    public static String PASSWORD;

    private static final String SPACE_NAME = "test__profile";
    private static final String PK_FIELD_NAME = "profile_id";

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        initClient();
    }

    public static void initClient() {
        client = TarantoolClientFactory.createClient()
            // If any addresses or an address provider are not specified,
            // the default host 127.0.0.1 and port 3301 are used
            .withAddress(container.getHost(), container.getPort())
            // For connecting to a Cartridge application, use the value of cluster_cookie parameter in the init.lua file
            .withCredentials(container.getUsername(), container.getPassword())
            // Specify using the default CRUD proxy operations mapping configuration
            .withProxyMethodMapping()
            // You may also specify more client settings, such as:
            // timeouts, number of connections, custom MessagePack entities to Java objects mapping, etc.
            .build();
    }

    @BeforeEach
    public void truncateSpace() {
        client.space(SPACE_NAME).truncate().join();
    }

    @Test
    public void clusterInsertSelectDeleteTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space = client.space(SPACE_NAME);

        // Use TarantoolTupleFactory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(
            client.getConfig().getMessagePackMapper());

        TarantoolTuple tuple;
        List<TarantoolTuple> tarantoolTuples = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            // Pass the field corresponding to bucket_id as null for tarantool/crud to compute it automatically
            tarantoolTuples.add(tupleFactory.create(1_000_000 + i, null, "FIO", 50 + i, 100 + i));
        }
        space.insertMany(tarantoolTuples).get();

        // Primary index key value will be determined from the tuple
        tuple = tupleFactory.create(1_000_000 - 1, null, "FIO", 50 - 1, 100 - 1);
        Conditions conditions = Conditions.after(tuple);

        TarantoolResult<TarantoolTuple> selectResult = space.select(conditions).get();
        assertEquals(20, selectResult.size());

        for (int i = 0; i < 20; i++) {
            tuple = selectResult.get(i);
            assertEquals(5, tuple.size());
            assertEquals(1_000_000 + i, tuple.getInteger(0));
            assertNotNull(tuple.getInteger(1)); //bucket_id
            assertEquals("FIO", tuple.getString(2));
            assertEquals(50 + i, tuple.getInteger(3));
            assertEquals(100 + i, tuple.getInteger(4));
        }

        conditions = Conditions.greaterOrEquals(PK_FIELD_NAME, 1_000_000 + 10);
        selectResult = space.select(conditions).get();
        assertEquals(10, selectResult.size());

        // space.delete can delete only a single tuple by primary index value
        // Do not use conditions like after or greaterOrEquals
        conditions = Conditions.equals(PK_FIELD_NAME, 1_000_000 + 15);
        TarantoolResult<TarantoolTuple> deleteResult = space.delete(conditions).get();
        assertEquals(1, deleteResult.size());

        conditions = Conditions.indexEquals(PK_FIELD_NAME, Collections.singletonList(1_000_000 + 10));
        TarantoolResult<TarantoolTuple> updateResult = space.update(conditions, TupleOperations.set(4, 10)).get();
        assertEquals(10, updateResult.get(0).getInteger(4));
    }

    @Test
    public void tarantoolTupleUsageExample() throws ExecutionException, InterruptedException, NullPointerException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> accounts =
            client.space("accounts");

        // Use TarantoolTupleFactory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(
            client.getConfig().getMessagePackMapper());

        // Create a tuple from listed values: [1, null, "credit card", 99.99]
        // This tuple contains java values
        TarantoolTuple inputTuple = tupleFactory.create(1, null, "credit card", 99.99);

        // Insert it in the database
        accounts.insert(inputTuple).join();

        // This tuple form the database
        Conditions conditions = Conditions.equals("id", 1);
        TarantoolResult<TarantoolTuple> selectResult = accounts.select(conditions).get();
        assertEquals(selectResult.size(), 1);
        // This tuple contains messagePack values
        TarantoolTuple selectTuple = selectResult.get(0);
        assertEquals(selectTuple.size(), 4);

        // You can get value from TarantoolTuple by its filedPosition
        // If you do not set objectClass default converter will be used for this value
        Optional<?> object = selectTuple.getObject(0);
        assertEquals(1, object.orElseThrow(NullPointerException::new));

        // For example any non-integer number will be converted to Double by default
        Optional<?> doubleValue = selectTuple.getObject(3);
        assertEquals(99.99, doubleValue.orElseThrow(NullPointerException::new));
        assertEquals(Double.class, doubleValue.orElseThrow(NullPointerException::new).getClass());

        // But if you need to get Float, you can set objectClass
        Optional<?> floatValue = selectTuple.getObject(3, Float.class);
        assertEquals(99.99f, floatValue.orElseThrow(NullPointerException::new));
        assertEquals(Float.class, floatValue.orElseThrow(NullPointerException::new).getClass());

        // You do not have to work with Optional
        // Getters for all basic types are available
        float floatNumber = selectTuple.getFloat(3);
        assertEquals(99.99f, floatNumber);

        // Also you can get values by field name
        Optional<?> balance = selectTuple.getObject("balance");
        assertEquals(99.99, balance.orElseThrow(NullPointerException::new));

        String stringValue = selectTuple.getString("name");
        assertEquals("credit card", stringValue);
    }
}
