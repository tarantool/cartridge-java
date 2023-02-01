package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Ivan Dneprov
 * <p>
 * WARNING: If you updated the code in this file, don't forget to update the docs/ProxyTarantoolClient.md permalinks!
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
        truncateSpace(SPACE_NAME);
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

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
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
}
