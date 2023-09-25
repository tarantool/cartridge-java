package io.tarantool.driver.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

@Fork(value = 1, jvmArgsAppend = "-Xmx1G")
public class ClusterBenchmarkRunner {
    private static final String TEST_SPACE = "test_space";
    private static final String TEST_PROFILE = "test__profile";

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @Measurement(iterations = 10)
    @OperationsPerInvocation(2)
    public void getSpaceObject(ClusterTarantoolSetup plan, Blackhole bh) {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            plan.tarantoolClient.space(TEST_SPACE);
        bh.consume(testSpace);

        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            plan.tarantoolClient.space(TEST_PROFILE);
        bh.consume(profileSpace);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 10)
    @OperationsPerInvocation(2000)
    public void writeData(ClusterTarantoolSetup plan, FuturesHolder futuresHolder, Spaces spaces, Blackhole bh) {
        // Fill 10000 rows into both spaces
        TarantoolTuple tarantoolTuple;
        String uuid;
        int nextId = 0;
        for (int i = 0; i < 1_000; i++) {
            uuid = UUID.randomUUID().toString();
            nextId = plan.nextTestSpaceId + i;
            tarantoolTuple = plan.tupleFactory.create(1_000_000 + nextId, null, uuid, 200_000 + nextId);
            futuresHolder.allFutures.add(spaces.testSpace.insert(tarantoolTuple));
            tarantoolTuple = plan.tupleFactory.create(1_000_000 + nextId, null, uuid, 50_000 + nextId, 100_000 + i);
            futuresHolder.allFutures.add(spaces.profileSpace.insert(tarantoolTuple));
        }
        nextId++;
        plan.nextTestSpaceId = nextId;
        plan.nextProfileSpaceId = nextId;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 10)
    @OperationsPerInvocation(1000)
    public void readDataUsingCallAPI(ClusterTarantoolSetup plan, FuturesHolder futuresHolder, Blackhole bh) {
        boolean coin = Math.random() - 0.5 > 0;
        String spaceName = coin ? TEST_SPACE : TEST_PROFILE;
        long nextId;
        for (int i = 0; i < 1_000; i++) {
            nextId = Math.round(Math.random() * 10_000) + 1_000_000;
            futuresHolder.allFutures.add(
                plan.tarantoolClient.callForSingleResult(
                    "custom_crud_get_one_record", Arrays.asList(spaceName, nextId), List.class)
            );
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 10)
    @OperationsPerInvocation(1000)
    public void readDataUsingSpaceAPI(
        ClusterTarantoolSetup plan, FuturesHolder futuresHolder, Spaces spaces, Blackhole bh) {
        boolean coin = Math.random() - 0.5 > 0;
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space = coin ?
            spaces.testSpace : spaces.profileSpace;
        String pkFieldName = coin ? "id" : "profile_id";
        long nextId;
        for (int i = 0; i < 1_000; i++) {
            nextId = Math.round(Math.random() * 10_000) + 1_000_000;
            futuresHolder.allFutures.add(
                space.select(Conditions.indexEquals(pkFieldName, Collections.singletonList(nextId)))
            );
        }
    }

    @State(Scope.Thread)
    public static class FuturesHolder {
        final List<CompletableFuture<?>> allFutures = new ArrayList<>(2_000);

        @Setup(Level.Invocation)
        public void doSetup() {
            allFutures.clear();
        }

        @TearDown(Level.Invocation)
        public void doTeardown() {
            allFutures.forEach(CompletableFuture::join);
        }
    }

    @State(Scope.Thread)
    public static class Spaces {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace;
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace;

        @Setup(Level.Iteration)
        public void doSetup(ClusterTarantoolSetup plan) {
            testSpace = plan.tarantoolClient.space(TEST_SPACE);
            profileSpace = plan.tarantoolClient.space(TEST_PROFILE);
        }
    }
}
