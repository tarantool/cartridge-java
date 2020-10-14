package io.tarantool.driver.integration;

import io.tarantool.driver.ProxyTarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolIndexType;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.operations.TupleOperations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Sergey Volgin
 */
@Testcontainers
public class ProxyTarantoolClientIT extends SharedCartridgeContainer {

    private static final String TEST_SPACE_NAME = "test__profile";

    private static ProxyTarantoolClient client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() {
        client = createClusterClient();
    }

    @Test
    public void getClusterMetaDataTest() {
        TarantoolMetadataOperations metadataOperations = client.metadata();

        Optional<TarantoolSpaceMetadata> spaceMeta = metadataOperations.getSpaceByName(TEST_SPACE_NAME);
        assertTrue(spaceMeta.isPresent());
        TarantoolSpaceMetadata spaceMetadata = spaceMeta.get();
        assertEquals(TEST_SPACE_NAME, spaceMetadata.getSpaceName());
        assertEquals(-1, spaceMetadata.getOwnerId());
        assertTrue(spaceMetadata.getSpaceId() > 0);
        assertEquals(5, spaceMetadata.getSpaceFormatMetadata().size());
        assertEquals(3, spaceMetadata.getFieldPositionByName("age"));

        Optional<TarantoolIndexMetadata> indexMeta = metadataOperations
                .getIndexByName(TEST_SPACE_NAME, "bucket_id");
        assertTrue(indexMeta.isPresent());
        TarantoolIndexMetadata indexMetadata = indexMeta.get();
        assertEquals("bucket_id", indexMetadata.getIndexName());
        assertEquals(1, indexMetadata.getIndexId());
        assertEquals(TarantoolIndexType.TREE, indexMetadata.getIndexType());
        assertFalse(indexMetadata.getIndexOptions().isUnique());
    }

    @Test
    public void clusterInsertSelectTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations profileSpace = client.space(TEST_SPACE_NAME);

        List<Object> values;
        TarantoolTuple tarantoolTuple;

        for (int i = 0; i < 20; i++) {
            values = Arrays.asList(1_000_000 + i, null, "FIO", 50 + i, 100 + i);
            tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
            profileSpace.insert(tarantoolTuple).get();
        }

        Conditions conditions = Conditions.greaterOrEquals("profile_id", 1_000_000);
        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(conditions).get();
        assertEquals(20, selectResult.size());

        TarantoolTuple tuple;
        for (int i = 0; i < 20; i++) {
            tuple = selectResult.get(i);
            assertEquals(5, tuple.size());
            assertEquals(1_000_000 + i, tuple.getInteger(0));
            assertNotNull(tuple.getInteger(1)); //bucket_id
            assertEquals("FIO", tuple.getString(2));
            assertEquals(50 + i, tuple.getInteger(3));
            assertEquals(100 + i, tuple.getInteger(4));
        }

        //insert by index
        conditions = Conditions.indexGreaterOrEquals("primary", Collections.singletonList(1_000_010));
        selectResult = profileSpace.select(conditions).get();
        assertEquals(10, selectResult.size());
    }

    @Test
    public void clusterInsertDeleteTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations profileSpace = client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(100, null, "fio", 10, 100);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(insertTuples.size(), 1);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 100);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "fio");

        values = Arrays.asList(2, null, "John Doe", 55, 99);
        tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
        insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(1, insertTuples.size());
        tuple = insertTuples.get(0);
        assertEquals(5, tuple.size());
        assertEquals(2, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals("John Doe", tuple.getString(2));

        Conditions conditions = Conditions.equals(0, 2);
        TarantoolResult<TarantoolTuple> deleteResult = profileSpace.delete(conditions).get();

        assertEquals(1, deleteResult.size());
        tuple = deleteResult.get(0);
        assertEquals(5, tuple.size());
        assertEquals(2, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals("John Doe", tuple.getString(2));
    }

    @Test
    public void replaceTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations profileSpace = client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(123, null, "Jane Doe", 18, 999);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(insertTuples.size(), 1);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 123);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "Jane Doe");
        assertEquals(tuple.getInteger(3), 18);
        assertEquals(tuple.getInteger(4), 999);

        List<Object> replaceValues = Arrays.asList(123, null, "John Doe", 21, 100);
        tarantoolTuple = new TarantoolTupleImpl(replaceValues, mapperFactory.defaultComplexTypesMapper());
        TarantoolResult<TarantoolTuple> replaceResult = profileSpace.replace(tarantoolTuple).get();
        assertEquals(replaceResult.size(), 1);
        tuple = replaceResult.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 123);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(tuple.getString(2), "John Doe");
        assertEquals(tuple.getInteger(3), 21);
        assertEquals(tuple.getInteger(4), 100);
    }

    @Test
    public void clusterUpdateTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations profileSpace = client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(223, null, "Jane Doe", 10, 10);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());

        TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
        assertEquals(insertTuples.size(), 1);
        TarantoolTuple tuple = insertTuples.get(0);
        assertEquals(tuple.size(), 5);
        assertEquals(tuple.getInteger(0), 223);
        assertNotNull(tuple.getInteger(1)); //bucket_id

        Conditions conditions = Conditions.indexEquals("profile_id", Collections.singletonList(223));

        TarantoolResult<TarantoolTuple> updateResult;

        updateResult = profileSpace.update(conditions, TupleOperations.add(3, 90).andAdd(4, 5)).get();
        assertEquals(223, updateResult.get(0).getInteger(0));
        assertEquals(100, updateResult.get(0).getInteger(3));
        assertEquals(15, updateResult.get(0).getInteger(4));
    }

    @Test
    public void clusterUpsertTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations profileSpace = client.space(TEST_SPACE_NAME);

        List<Object> values = Arrays.asList(301, null, "Jack Sparrow", 30, 0);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());

        Conditions conditions = Conditions.equals("profile_id", 301);

        TarantoolResult<TarantoolTuple> upsertResult;

        //first time tuple not exist
        profileSpace.upsert(conditions,
                tarantoolTuple, TupleOperations.add(3, 5).andBitwiseOr(4, 7)).get();

        upsertResult = profileSpace.select(conditions).get();
        assertEquals(1, upsertResult.size());
        TarantoolTuple tuple = upsertResult.get(0);
        assertEquals(5, tuple.size());
        assertEquals(301, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(30, upsertResult.get(0).getInteger(3));
        assertEquals(0, upsertResult.get(0).getInteger(4));

        //second time tuple exist
        profileSpace.upsert(conditions,
                tarantoolTuple, TupleOperations.add(3, 5).andBitwiseOr(4, 7)).get();

        upsertResult = profileSpace.select(conditions).get();
        tuple = upsertResult.get(0);
        assertEquals(tuple.getInteger(0), 301);
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals(35, upsertResult.get(0).getInteger(3));
        assertEquals(7, upsertResult.get(0).getInteger(4));
    }
}
