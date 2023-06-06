package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.api.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.core.metadata.SpacesTarantoolMetadataContainer;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class DLGGetSchemaIT {

    private static final Logger log = LoggerFactory.getLogger(DLGGetSchemaIT.class);

    @Container
    private static final TarantoolContainer container = new TarantoolContainer()
        .withScriptFileName("numerical-index-name.lua")
        .withLogConsumer(new Slf4jLogConsumer(log));

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void prepareCluster() {
        container.start();
    }

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
        return TarantoolClientFactory.createClient()
            .withAddress(container.getHost(), container.getPort())
            .withCredentials(container.getUsername(), container.getPassword())
            .build();
    }

    @Test
    public void simplePutAndGetTest() throws Exception {
        try (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = setupClient()) {
            TarantoolMetadataProvider metadataProvider = client.metadataProvider();
            TarantoolMetadataContainer metadata = metadataProvider.getMetadata().join();
            Map<String, Map<Object, TarantoolIndexMetadata>> indexMetadata = metadata.getIndexMetadataBySpaceName();
            Map indexMap = indexMetadata.get("region");
        }
    }
}
