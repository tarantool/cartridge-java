package io.tarantool.driver.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Fork(value = 1, jvmArgsAppend = {
    "-Xms1G", "-Xmx1G", "-XX:+UseG1GC", //"-XX:+UnlockCommercialFeatures",
    "-XX:+FlightRecorder",
    "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "-XX:StartFlightRecording=settings=/home/fedora/sources/cartridge-java/profile.jfc"
})
public class ClusterBenchmarkRunner {
    private static final String TEST_SPACE = "test_space";
    private static final String TEST_PROFILE = "test__profile";
    private static final String CLUSTER_INIT_SCRIPT = "org/testcontainers/containers/cluster_benchmark.lua";

    private static Logger logger = LoggerFactory.getLogger(ClusterBenchmarkRunner.class);

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 2)
    @Measurement(iterations = 10)
    @OperationsPerInvocation(2000)
    public void writeDataUsingCallAPI(ClusterTarantoolSetup plan, FuturesHolder futuresHolder) {
        // Fill 10000 rows into both spaces
        TarantoolTuple tarantoolTuple;
        String uuid;
        int nextId = 0;
        for (int i = 0; i < 1_000; i++) {
            uuid = UUID.randomUUID().toString();
            nextId = plan.nextTestSpaceId + i;
            tarantoolTuple = plan.tupleFactory.create(1_000_000 + nextId, null, uuid, 200_000 + nextId);
            futuresHolder.allFutures.add(plan.tarantoolClient.callForSingleResult(
                "custom_crud_insert_one_record" , Arrays.asList(TEST_SPACE, tarantoolTuple), Map.class));
            tarantoolTuple = plan.tupleFactory.create(1_000_000 + nextId, null, uuid, 50_000 + nextId, 100_000 + i);
            futuresHolder.allFutures.add(plan.tarantoolClient.callForSingleResult(
                "custom_crud_insert_one_record" , Arrays.asList(TEST_PROFILE, tarantoolTuple), Map.class));
        }
        nextId++;
        plan.nextTestSpaceId = nextId;
        plan.nextProfileSpaceId = nextId;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 2)
    @Measurement(iterations = 10)
    @OperationsPerInvocation(2000)
    public void writeDataUsingSpaceAPI(
        ClusterTarantoolSetup plan, FuturesHolder futuresHolder, Spaces spaces) {
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
    @Warmup(iterations = 2)
    @Measurement(iterations = 10)
    @OperationsPerInvocation(1000)
    public void readDataUsingCallAPI(
        ClusterTarantoolSetup plan, FuturesHolder futuresHolder, ClusterDataInit clusterDataInit) {
        boolean coin;
        String spaceName;
        long nextId;
        for (int i = 0; i < 1_000; i++) {
            nextId = Math.round(Math.random() * 10_000) + 1_000_000;
            coin = Math.random() - 0.5 > 0;
            spaceName = coin ? TEST_SPACE : TEST_PROFILE;
            futuresHolder.allFutures.add(
                plan.tarantoolClient.callForSingleResult(
                    "custom_crud_get_one_record", Arrays.asList(spaceName, nextId), List.class)
            );
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 2)
    @Measurement(iterations = 10)
    @OperationsPerInvocation(1000)
    public void readDataUsingSpaceAPI(
        ClusterTarantoolSetup plan, FuturesHolder futuresHolder, Spaces spaces, ClusterDataInit clusterDataInit) {
        boolean coin;
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> space;
        String pkFieldName;
        long nextId;
        for (int i = 0; i < 1_000; i++) {
            nextId = Math.round(Math.random() * 10_000) + 1_000_000;
            coin = Math.random() - 0.5 > 0;
            space = coin ? spaces.testSpace : spaces.profileSpace;
            pkFieldName = coin ? "id" : "profile_id";
            futuresHolder.allFutures.add(
                space.select(Conditions.indexEquals(pkFieldName, Collections.singletonList(nextId)))
            );
        }
    }

    @State(Scope.Thread)
    public static class FuturesHolder {
        final List<CompletableFuture<?>> allFutures = new ArrayList<>(2_000);

        @TearDown(Level.Invocation)
        public void doTeardown() {
            try {
                allFutures.forEach(CompletableFuture::join);
                allFutures.clear();
            } catch (Exception e) {
                e.printStackTrace();
            }
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

    @State(Scope.Thread)
    public static class ClusterDataInit {

        @Setup(Level.Iteration)
        public void doSetup(ClusterTarantoolSetup plan) throws Exception {
            plan.tarantoolContainer.executeScript(CLUSTER_INIT_SCRIPT);
        }
    }
}
