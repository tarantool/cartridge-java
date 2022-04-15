package io.tarantool.driver.benchmark;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.DefaultResultMapperFactoryFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

@State(Scope.Benchmark)
public class TarantoolSetup {
    public Logger log = LoggerFactory.getLogger(TarantoolSetup.class);

    public TarantoolContainer tarantoolContainer = new TarantoolContainer()
            .withScriptFileName("org/testcontainers/containers/benchmark.lua")
            .withLogConsumer(new Slf4jLogConsumer(log));

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    MessagePackMapper defaultMapper;
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
            resultMapper;

    private void initClient() {
        log.info("Attempting connect to Tarantool");
        tarantoolClient = TarantoolClientFactory.createClient()
                .withAddress(tarantoolContainer.getHost(), tarantoolContainer.getPort())
                .withCredentials(tarantoolContainer.getUsername(), tarantoolContainer.getPassword())
                .build();

        DefaultResultMapperFactoryFactory factory = new DefaultResultMapperFactoryFactory();
        TarantoolSpaceMetadata spaceMetadata = tarantoolClient.metadata().getSpaceByName("test_space").get();

        defaultMapper = tarantoolClient.getConfig().getMessagePackMapper();
        resultMapper = factory
                .defaultTupleSingleResultMapperFactory()
                .withDefaultTupleValueConverter(defaultMapper, spaceMetadata);

        log.info("Successfully connected to Tarantool, version = {}", tarantoolClient.getVersion());
    }

    @Setup(Level.Trial)
    public void doSetup() {
        System.out.println("Do Setup");
        if (!tarantoolContainer.isRunning()) {
            tarantoolContainer.start();
        }
        initClient();
    }

    @TearDown(Level.Trial)
    public void doTearDown() throws Exception {
        System.out.println("Do TearDown");
        tarantoolClient.close();
        tarantoolContainer.close();
    }
}
