package io.tarantool.driver.benchmark;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BenchmarkRunner {
    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }

    @Benchmark
    @Fork(1)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(1000)
    public void acceptingDiffTypes(TarantoolSetup plan, Blackhole bh) {
        List<?> result = plan.tarantoolClient.call(
                "return_arrays_with_different_types"
        ).join();

        assertEquals(1, result.size());
        List<List<?>> tuples = (List<List<?>>) result.get(0);
        assertEquals(1000, tuples.size());
        int i = 1;
        ArrayList<Object> list = new ArrayList<>(Arrays.asList(0, "asdsad", 1, false, 2.2));
        HashMap<String, Object> map = new HashMap<>();
        map.put("hello", "world");
        map.put("d", 3);
        for (List tuple : tuples) {
            assertEquals(i++, tuple.get(0));
            assertEquals("aaaaaaaa", tuple.get(1));
            assertNull(tuple.get(2));
            assertEquals("bbbbb", tuple.get(3));
            assertEquals(false, tuple.get(4));
            assertEquals(99, tuple.get(5));
            assertEquals("cccccc", tuple.get(6));
            assertEquals(3.4654, tuple.get(7));
            assertEquals("a", tuple.get(8));
            assertEquals(-123312, tuple.get(9));
            assertEquals(list, tuple.get(10));
            assertEquals(map, tuple.get(11));
            assertEquals(true, tuple.get(12));
            assertEquals(9223372036854775807L, tuple.get(13));
            assertEquals(-9223372036854775807L, tuple.get(14));
        }
        bh.consume(result);
    }

    @Benchmark
    @Fork(1)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(1000)
    public void acceptingDiffTypesAsTuplesAndUnpackIt(TarantoolSetup plan, Blackhole bh) {
        TarantoolResult<TarantoolTuple> tuples = plan.tarantoolClient.call(
                "return_arrays_with_different_types",
                Collections.emptyList(),
                plan.defaultMapper,
                plan.resultMapper
        ).join();

        assertEquals(1000, tuples.size());
        int i = 1;
        ArrayList<Object> list = new ArrayList<>(Arrays.asList(0, "asdsad", 1, false, 2.2));
        HashMap<String, Object> map = new HashMap<>();
        map.put("hello", "world");
        map.put("d", 3);
        for (TarantoolTuple tuple : tuples) {
            assertEquals(i++, tuple.getObject("field1").get());
            assertEquals("aaaaaaaa", tuple.getObject("field2").get());
            assertFalse(tuple.getObject("field3").isPresent());
            assertEquals("bbbbb", tuple.getObject("field4").get());
            assertEquals(false, tuple.getObject("field5").get());
            assertEquals(99, tuple.getObject("field6").get());
            assertEquals("cccccc", tuple.getObject("field7").get());
            assertEquals(3.4654, tuple.getObject("field8").get());
            assertEquals("a", tuple.getObject("field9").get());
            assertEquals(-123312, tuple.getObject("field10").get());
            assertEquals(list, tuple.getObject("field11").get());
            assertEquals(map, tuple.getObject("field12").get());
            assertEquals(true, tuple.getObject("field13").get());
            assertEquals(9223372036854775807L, tuple.getObject("field14").get());
            assertEquals(-9223372036854775807L, tuple.getObject("field15").get());
        }
        bh.consume(tuples);
    }

    @Benchmark
    @Fork(1)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(1000)
    public void acceptingDiffTypesAsTuplesAndUnpackItWithTargetType(TarantoolSetup plan, Blackhole bh) {
        TarantoolResult<TarantoolTuple> tuples = plan.tarantoolClient.call(
                "return_arrays_with_different_types",
                Collections.emptyList(),
                plan.defaultMapper,
                plan.resultMapper
        ).join();

        assertEquals(1000, tuples.size());
        int i = 1;
        ArrayList<Object> list = new ArrayList<>(Arrays.asList(0, "asdsad", 1, false, 2.2));
        HashMap<String, Object> map = new HashMap<>();
        map.put("hello", "world");
        map.put("d", 3);
        for (TarantoolTuple tuple : tuples) {
            assertEquals(i++, tuple.getObject("field1", Integer.class).get());
            assertEquals("aaaaaaaa", tuple.getObject("field2", String.class).get());
            assertFalse(tuple.getObject("field3", String.class).isPresent());
            assertEquals("bbbbb", tuple.getObject("field4", String.class).get());
            assertEquals(false, tuple.getObject("field5", Boolean.class).get());
            assertEquals(99, tuple.getObject("field6", Integer.class).get());
            assertEquals("cccccc", tuple.getObject("field7", String.class).get());
            assertEquals(3.4654, tuple.getObject("field8", Double.class).get());
            assertEquals("a", tuple.getObject("field9", String.class).get());
            assertEquals(-123312, tuple.getObject("field10", Integer.class).get());
            assertEquals(list, tuple.getObject("field11", List.class).get());
            assertEquals(map, tuple.getObject("field12", Map.class).get());
            assertEquals(true, tuple.getObject("field13", Boolean.class).get());
            assertEquals(9223372036854775807L, tuple.getObject("field14", Long.class).get());
            assertEquals(-9223372036854775807L, tuple.getObject("field15", Long.class).get());
        }
        bh.consume(tuples);
    }

    @Benchmark
    @Fork(1)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(1000)
    public void passingArrayOfArraysWithDiffTypes(TarantoolSetup plan, Blackhole bh) {
        bh.consume(plan.tarantoolClient.call(
                "empty_function", plan.arraysWithDiffElements).join());
    }

    @Benchmark
    @Fork(1)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(1000)
    public void passingArrayOfArraysWithNestedArrays(TarantoolSetup plan, Blackhole bh) {
        bh.consume(plan.tarantoolClient.call(
                "empty_function", plan.arraysWithNestedArrays).join());
    }

    @Benchmark
    @Fork(1)
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(1000)
    public void passingArrayOfArraysWithNestedMaps(TarantoolSetup plan, Blackhole bh) {
        bh.consume(plan.tarantoolClient.call(
                "empty_function", plan.arraysWithNestedMaps).join());
    }
}
