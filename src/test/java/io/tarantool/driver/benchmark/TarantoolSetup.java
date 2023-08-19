package io.tarantool.driver.benchmark;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactory;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactoryImpl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

@State(Scope.Benchmark)
public class TarantoolSetup {
    public Logger log = LoggerFactory.getLogger(TarantoolSetup.class);

    public TarantoolContainer tarantoolContainer = new TarantoolContainer()
        .withScriptFileName("org/testcontainers/containers/benchmark.lua")
        .withLogConsumer(new Slf4jLogConsumer(log));

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> retryingTarantoolClient;
    MessagePackMapper defaultMapper;
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
        resultMapper;
    Supplier<CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>>
        resultMapperSupplier;
    List<List<Object>> arraysWithDiffElements;
    List<List<Object>> arraysWithNestedArrays;
    List<List<Object>> arraysWithNestedMaps;

    private void initClient() {
        log.info("Attempting connect to Tarantool");
        tarantoolClient = TarantoolClientFactory.createClient()
            .withAddress(tarantoolContainer.getHost(), tarantoolContainer.getPort())
            .withCredentials(tarantoolContainer.getUsername(), tarantoolContainer.getPassword())
            .build();

        retryingTarantoolClient
            = TarantoolClientFactory.configureClient(tarantoolClient).withRetryingByNumberOfAttempts(3).build();

        TarantoolTupleResultMapperFactory tarantoolTupleResultMapperFactory =
            TarantoolTupleResultMapperFactoryImpl.getInstance();

        TarantoolSpaceMetadata spaceMetadata = tarantoolClient.metadata().getSpaceByName("test_space").get();

        defaultMapper = tarantoolClient.getConfig().getMessagePackMapper();
        resultMapper = tarantoolTupleResultMapperFactory
            .withSingleValueArrayToTarantoolTupleResultMapper(defaultMapper, spaceMetadata);
        resultMapperSupplier = () -> resultMapper;

        log.info("Successfully connected to Tarantool, version = {}", tarantoolClient.getVersion());
    }

    private void initInput() {
        arraysWithDiffElements = new ArrayList<>();
        arraysWithNestedArrays = new ArrayList<>();
        arraysWithNestedMaps = new ArrayList<>();

        HashMap<String, Object> hm = new HashMap<String, Object>();
        hm.put("hello", "world");
        hm.put("d", 2);

        List<Object> arrayOfDiffElements = new ArrayList<>(Arrays.asList(
            'a',
            null,
            "bbbbb",
            false,
            99,
            "cccccc",
            3.4654F,
            3.4654D,
            'a',
            -123312,
            new ArrayList<>(Arrays.asList(0, "asdsad", 1, false, 2.2)),
            hm,
            true,
            9223372036854775807L,
            -9223372036854775807L
        ));

        List<Object> arrayOfNestedArrays = new ArrayList<>();
        List<Object> arrayOfNestedMaps = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            arrayOfNestedArrays.add(new ArrayList<>());
            arrayOfNestedMaps.add(new HashMap<>());
        }

        for (int i = 0; i < 1000; i++) {
            arraysWithDiffElements.add(arrayOfDiffElements);
            arraysWithNestedArrays.add(arrayOfNestedArrays);
            arraysWithNestedMaps.add(arrayOfNestedMaps);
        }
    }

    @Setup(Level.Trial)
    public void doSetup() {
        System.out.println("Do Setup");
        if (!tarantoolContainer.isRunning()) {
            tarantoolContainer.start();
        }
        initClient();
        initInput();
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws Exception {
        System.out.println("Do TearDown");
        tarantoolClient.close();
        tarantoolContainer.close();
    }
}
