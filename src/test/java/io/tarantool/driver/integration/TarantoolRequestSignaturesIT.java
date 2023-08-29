package io.tarantool.driver.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Alexey Kuzin
 */
public class TarantoolRequestSignaturesIT extends SharedCartridgeContainer {
    private static final String TEST_SPACE = "test_space";
    private static final String TEST_PROFILE = "test__profile";

    private static final MessagePackMapper defaultMapper =
        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private static final TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(defaultMapper);
    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        initClient();
        truncateSpace(TEST_SPACE);
        truncateSpace(TEST_PROFILE);
    }

    public static void initClient() {
        client = TarantoolClientFactory.createClient()
            .withAddresses(
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3301)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3302)),
                new TarantoolServerAddress(container.getRouterHost(), container.getMappedPort(3303))
            )
            .withCredentials(container.getUsername(), container.getPassword())
            .withConnections(10)
            .withEventLoopThreadsNumber(10)
            .withRequestTimeout(10000)
            .withProxyMethodMapping()
            .build();
    }

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
    }

    @Test
    public void test_cachingWithRequestSignatures_shouldNotProvideCollisions() throws InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE);
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_PROFILE);

        // Fill 10000 rows into both spaces
        TarantoolTuple tarantoolTuple;
        String uuid;
        List<CompletableFuture<?>> allFutures = new ArrayList<>(20_000);
        for (int i = 0; i < 10_000; i++) {
            uuid = UUID.randomUUID().toString();
            tarantoolTuple = tupleFactory.create(1_000_000 + i, null, uuid, 200_000 + i);
            allFutures.add(testSpace.insert(tarantoolTuple));
            tarantoolTuple = tupleFactory.create(1_000_000 + i, null, uuid, 50_000 + i, 100_000 + i);
            allFutures.add(profileSpace.insert(tarantoolTuple));
            if (i % 2000 == 0) {
                allFutures.forEach(CompletableFuture::join); // to not catch storage timeouts
                allFutures.clear();
            }
        }
        allFutures.forEach(CompletableFuture::join);

        // Make requests concurrently
        allFutures.clear();
        int nextTestSpaceId = 10_000;
        AtomicLong testSpaceSum = new AtomicLong(0);
        AtomicInteger testCounter = new AtomicInteger(0);
        int nextProfileSpaceId = 10_000;
        AtomicLong profileSpaceSum = new AtomicLong(0);
        AtomicInteger profileCounter = new AtomicInteger(0);
        boolean coin;
        int nextId;
        while (nextTestSpaceId > 0 || nextProfileSpaceId > 0) {
            coin = Math.random() - 0.5 > 0;
            if (coin && nextTestSpaceId > 0 || nextProfileSpaceId <= 0) {
                nextId = 1_000_000 + --nextTestSpaceId;
                allFutures.add(
                    client.callForSingleResult(
                        "custom_crud_get_one_record", Arrays.asList(TEST_SPACE, nextId), List.class)
                        .thenAccept(t -> {
                            testSpaceSum.getAndAdd((Integer) t.get(3));
                            testCounter.getAndIncrement();
                        })
                );
            } else {
                nextId = 1_000_000 + --nextProfileSpaceId;
                allFutures.add(
                    // this method actually calls callForSingleResult inside too but with different mapper stack
                    profileSpace.select(Conditions.indexEquals("profile_id", Collections.singletonList(nextId)))
                        .thenAccept(t -> {
                            profileSpaceSum.getAndAdd(t.get(0).getInteger("balance"));
                            profileCounter.getAndIncrement();
                        })
                );
            }
        }
        allFutures.forEach(CompletableFuture::join);

        assertEquals(10_000, testCounter.get());
        assertEquals(10_000, profileCounter.get());

        // Check that all requests returned correct values
        int baseSum = 9999 * 10000 / 2;
        assertEquals(2_000_000_000L + baseSum, testSpaceSum.get());
        assertEquals(1_000_000_000L + baseSum, profileSpaceSum.get());
    }
}
