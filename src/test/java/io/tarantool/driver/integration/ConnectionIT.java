package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.exceptions.NoAvailableConnectionsException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolServerException;
import io.tarantool.driver.exceptions.TarantoolSocketException;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
@Testcontainers
public class ConnectionIT {
    private static final String TEST_SPACE_NAME = "test_space";
    private static final Logger log = LoggerFactory.getLogger(ConnectionIT.class);

    @Container
    private static final TarantoolContainer tarantoolContainer = new TarantoolContainer()
            .withScriptFileName("org/testcontainers/containers/server.lua");

    @Test
    public void connectAndCheckMetadata() throws Exception {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), tarantoolContainer.getPort());

        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            ExecutorService executor = Executors.newFixedThreadPool(10);
            List<CompletableFuture<?>> futures = new ArrayList<>(10);
            for (int i = 0; i < 10; i++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    Optional<TarantoolSpaceMetadata> spaceHolder = client.metadata().getSpaceByName("_space");
                    assertTrue(spaceHolder.isPresent(), "Failed to get space metadata");
                }, executor));
            }
            futures.forEach(CompletableFuture::join);

            Optional<TarantoolSpaceMetadata> spaceMetadata = client.metadata().getSpaceByName(TEST_SPACE_NAME);
            assertTrue(spaceMetadata.isPresent(), String.format("Failed to get '%s' metadata", TEST_SPACE_NAME));
            assertEquals(TEST_SPACE_NAME, spaceMetadata.get().getSpaceName());
            log.info("Retrieved ID from metadata for space '{}': {}",
                    spaceMetadata.get().getSpaceName(), spaceMetadata.get().getSpaceId());
            assertTrue(spaceMetadata.get().getFieldByName("year").get().getIsNullable());
        }
    }

    private CompletableFuture<List<?>> connectAndEval(String command) throws Exception {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), tarantoolContainer.getPort());

        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            return client.eval(command);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void connectAndCloseAfterFuture() throws Exception {
        List<?> result = connectAndEval("return 1, 2").get(1000, TimeUnit.MILLISECONDS);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0));
        assertEquals(2, result.get(1));
    }

    @Test
    public void testIncorrectHostname_shouldThrowException() {
        assertThrows(TarantoolClientException.class, () -> {
            TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                    tarantoolContainer.getUsername(), tarantoolContainer.getPassword());
            TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                    "wronghost", tarantoolContainer.getPort());
            ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress);
            // Connection is actually performed here
            client.getVersion();
        });
    }

    @Test
    public void testIncorrectPort_shouldThrowException() {
        assertThrows(TarantoolClientException.class, () -> {
            TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                    tarantoolContainer.getUsername(), tarantoolContainer.getPassword());
            TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                    tarantoolContainer.getHost(), 9999);
            ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress);
            // Connection is actually performed here
            client.getVersion();
        });
    }

    @Test
    public void testCloseAfterIncorrectPort_shouldThrowException() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), 9999);
        assertThrows(TarantoolClientException.class, () -> {
            try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
                // Connection is actually performed here
                client.getVersion();
            }
        });
    }

    @Test
    public void testCloseAfterIncorrectPassword_shouldThrowException() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), "incorrect");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), tarantoolContainer.getPort());
        assertThrows(TarantoolClientException.class, () -> {
            try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
                // Connection is actually performed here
                client.getVersion();
            }
        });
    }

    @Test
    public void testIncorrectPassword_secondRequestShouldNotHang() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), "incorrect");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), tarantoolContainer.getPort());
        assertThrows(TarantoolClientException.class, () -> {
            try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
                // Connection is actually performed here
                try {
                    client.getVersion();
                } catch (TarantoolServerException e) {
                    log.error("Caught exception", e);
                }
                // the second request should not hang, but should throw the exception
                client.metadata().getSpaceByName("_vspace");
            }
        });
    }

    @Test
    public void testIncorrectPassword_multipleRequestsShouldNotHang() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), "incorrect");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                "neverwhere", tarantoolContainer.getPort());
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withConnectTimeout(100)
                .withRequestTimeout(100)
                .withCredentials(credentials)
                .withConnections(10)
                .build();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        assertThrows(ExecutionException.class, () -> {
            try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(config, serverAddress)) {
                // Connection is actually performed here
                List<CompletableFuture<?>> futures = new ArrayList<>(10);
                for (int i = 0; i < 10; i++) {
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            CompletableFuture<TarantoolResult<TarantoolTuple>> result =
                                    client.space("_vspace").select(Conditions.any());
                            result.get(100, TimeUnit.MILLISECONDS);
                        } catch (TarantoolServerException | NoAvailableConnectionsException e) {
                            log.error("Caught exception {}", e.getMessage());
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    }, executor));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[]{}))
                        .get(1000, TimeUnit.MILLISECONDS);
            }
        });
    }
}
