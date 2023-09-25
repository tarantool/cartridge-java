package io.tarantool.driver.benchmark;

import java.time.Duration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolCartridgeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

@State(Scope.Benchmark)
public class ClusterTarantoolSetup {
    public Logger logger = LoggerFactory.getLogger(ClusterTarantoolSetup.class);

    final MessagePackMapper defaultMapper =
        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    final TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(defaultMapper);

    final TarantoolCartridgeContainer tarantoolContainer =
        new TarantoolCartridgeContainer(
            "Dockerfile",
            "cartridge-java-test",
            "cartridge/instances.yml",
            "cartridge/topology.lua")
            .withDirectoryBinding("cartridge")
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .waitingFor(Wait.forLogMessage(".*Listening HTTP on.*", 5))
            .withStartupTimeout(Duration.ofMinutes(2));

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;

    int nextTestSpaceId;
    int nextProfileSpaceId;

    private void initClient() {
        tarantoolClient = TarantoolClientFactory.createClient()
            .withAddresses(
                new TarantoolServerAddress(tarantoolContainer.getRouterHost(), tarantoolContainer.getMappedPort(3301)),
                new TarantoolServerAddress(tarantoolContainer.getRouterHost(), tarantoolContainer.getMappedPort(3302)),
                new TarantoolServerAddress(tarantoolContainer.getRouterHost(), tarantoolContainer.getMappedPort(3303))
            )
            .withCredentials(tarantoolContainer.getUsername(), tarantoolContainer.getPassword())
            .withConnections(10)
            .withEventLoopThreadsNumber(10)
            .withRequestTimeout(10000)
            .withProxyMethodMapping()
            .build();
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
