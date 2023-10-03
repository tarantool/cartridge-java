package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * @author Ivan Dneprov
 * <p>
 * WARNING: If you updated the code in this file, don't forget to update the readme permalinks!
 */
@Testcontainers
public class SingleInstanceExampleIT {

    private static final Logger log = LoggerFactory.getLogger(SingleInstanceExampleIT.class);

    @Container
    private static final TarantoolContainer container = new TarantoolContainer()
        .withScriptFileName("single-instance.lua")
        .withLogConsumer(new Slf4jLogConsumer(log));

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void prepareCluster() {
        container.start();
    }

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
        return TarantoolClientFactory.createClient()
            // If any addresses or an address provider are not specified,
            // the default host 127.0.0.1 and port 3301 are used
            .withAddress(container.getHost(), container.getPort())
            // For connecting to a Cartridge application, use the value of cluster_cookie parameter in the init.lua file
            .withCredentials(container.getUsername(), container.getPassword())
            // you may also specify more client settings, such as:
            // timeouts, number of connections, custom MessagePack entities to Java objects mapping, etc.
            .build();
    }

    @Test
    public void simplePutAndGetTest() throws Exception {
        try (TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = setupClient()) {
            TarantoolTupleFactory tupleFactory =
                new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
            TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> regionSpace =
                client.space("region");

            TarantoolTuple tarantoolTuple = tupleFactory.create(Arrays.asList(77, "Moscow"));
            TarantoolResult<TarantoolTuple> insertTuples = regionSpace.insert(tarantoolTuple).get();
            TarantoolResult<TarantoolTuple> selectTuples =
                regionSpace.select(Conditions.equals("id", 77)).get();

            TarantoolTuple insertTuple = insertTuples.get(0);
            TarantoolTuple selectTuple = selectTuples.get(0);
            assertEquals(insertTuple.getInteger("id"), selectTuple.getInteger("id"));
            assertEquals(insertTuple.getString("name"), selectTuple.getString("name"));
        }
    }
}
