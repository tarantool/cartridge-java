package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.exceptions.NoAvailableConnectionsException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolConnectionException;
import io.tarantool.driver.exceptions.TarantoolInternalException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.util.ClassUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
@Testcontainers
public class ConnectionIT extends SharedTarantoolContainer {
    private static final String TEST_SPACE_NAME = "test_space";
    private static final String GUEST_USER = "guest";
    private static final String EXISTING_USER_WITHOUT_PASSWORD = "empty_password_user";
    private static final Logger log = LoggerFactory.getLogger(ConnectionIT.class);

    @BeforeAll
    public static void setUp() {
        startContainer();
    }

    @Test
    public void connectAndCheckMetadata() throws Exception {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            container.getUsername(), container.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());

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
            container.getUsername(), container.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());

        ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress);
        CompletableFuture<List<?>> future = client.eval(command);
        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(100);
                client.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        future = closeFuture.thenCombine(future, (o, r) -> r);
        return future;
    }

    @Test
    public void connectAndCloseAfterFuture() throws Exception {
        List<?> result = connectAndEval("require('fiber').sleep(0.1) return 1, 2").get(1000, TimeUnit.MILLISECONDS);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0));
        assertEquals(2, result.get(1));
    }

    @Test
    public void testGuestExplicit() throws Exception {
        // withName
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            GUEST_USER, "");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            List<?> resultList = client.eval("return box.session.user()").get();
            String sessionUser = (String) resultList.get(0);
            assertEquals(GUEST_USER, sessionUser);
        }
    }

    @Test
    public void testGuestExplicitWithPassword_shouldThrowException() throws Exception {
        // withName
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            GUEST_USER, "abcd");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> {
                client.eval("return box.session.user()").get();
            });
            // In the logs:
            // .TarantoolInternalException: InnerErrorMessage:
            // code: 47
            // message: Incorrect password supplied for user 'guest'
            assertTrue(e.getCause() instanceof TarantoolConnectionException);
        }
    }

    @Test
    public void testGuestImplicit() throws Exception {
        // withoutName
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            "", "");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            List<?> resultList = client.eval("return box.session.user()").get();
            String sessionUser = (String) resultList.get(0);
            assertEquals(GUEST_USER, sessionUser);
        }
    }

    @Test
    public void testGuestImplicitWithPassword_shouldThrowException() throws Exception {
        // withoutName
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            "", "abcd");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> {
                List<?> resultList = client.eval("return box.session.user()").get();
                String sessionUser = (String) resultList.get(0);
                assertEquals(GUEST_USER, sessionUser);
            });
            // In the logs:
            // TarantoolBadCredentialsException: Bad credentials
            assertTrue(e.getCause() instanceof TarantoolConnectionException);
        }
    }

    @Test
    public void testExistingUserWithoutPassword() throws Exception {
        // The EXISTING_USER_WITHOUT_PASSWORD was specially created for this test in a lua script
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            EXISTING_USER_WITHOUT_PASSWORD, "");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            List<?> resultList = client.eval("return box.session.user()").get();
            String sessionUser = (String) resultList.get(0);
            assertEquals(EXISTING_USER_WITHOUT_PASSWORD, sessionUser);
        }
    }

    @Test
    public void testRandomNameUserWithoutPassword_shouldThrowException() throws Exception {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            "RandomUserName", "");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> {
                client.eval("return box.session.user()").get();
            });
            // In the logs we see an inner tarantool error message
            // "User 'RandomUserName' is not found"
            assertTrue(e.getCause() instanceof TarantoolConnectionException);
        }
    }

    @Test
    public void testIncorrectHostname_shouldThrowException() {
        assertThrows(TarantoolClientException.class, () -> {
            TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                container.getUsername(), container.getPassword());
            TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                "wronghost", container.getPort());
            ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress);
            // Connection is actually performed here
            client.getVersion();
        });
    }

    @Test
    public void testIncorrectPort_shouldThrowException() {
        assertThrows(TarantoolClientException.class, () -> {
            TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                container.getUsername(), container.getPassword());
            TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                container.getHost(), 9999);
            ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress);
            // Connection is actually performed here
            client.getVersion();
        });
    }

    @Test
    public void testCloseAfterIncorrectPort_shouldThrowException() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            container.getUsername(), container.getPassword());
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), 9999);
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
            container.getUsername(), "incorrect");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
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
            container.getUsername(), "incorrect");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        assertThrows(TarantoolClientException.class, () -> {
            try (ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress)) {
                // Connection is actually performed here
                try {
                    client.getVersion();
                } catch (TarantoolInternalException e) {
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
            container.getUsername(), "incorrect");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            "neverwhere", container.getPort());
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
                        } catch (TarantoolInternalException | NoAvailableConnectionsException e) {
                            log.error("Caught exception {}", e.getMessage());
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }
                    }, executor));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[]{}))
                    .get(5000, TimeUnit.MILLISECONDS);
            }
        });
    }

    @Test
    public void testIncorrectPassword_shouldCloseConnection() throws Exception {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            container.getUsername(), "incorrect");
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());
        ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress);
        for (int i = 0; i < 100; i++) {
            assertThrows(TarantoolClientException.class, () -> {
                client.getVersion();
            });
        }
        Map netStat = (Map) container.executeCommand("return box.stat.net()").join().get(0);
        Map connections = (Map) netStat.get("CONNECTIONS");
        assertTrue((Integer) connections.get("current") <= 2); // one for container one for static test client
    }

    @Test
    public void testClientClosing_clientWithoutConnectionsShouldNotHang() throws Exception {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
            container.getUsername(), container.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
            container.getHost(), container.getPort());

        ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(credentials, serverAddress);
        client.close();
    }

    @Test
    public void test_AddressProviderReturnsNull_shouldThrowTarantoolClientException() {
        // given
        TarantoolClusterAddressProvider addressProvider = () -> null;

        //when
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
            .withAddressProvider(addressProvider)
            .withCredentials(new SimpleTarantoolCredentials(GUEST_USER, ""))
            .withConnectTimeout(1000)
            .withReadTimeout(1000)
            .withConnections(1)
            .build();

        // then
        TarantoolClientException exception = assertThrows(TarantoolClientException.class, client::getVersion);
        assertFalse(ClassUtil.getRootCause(exception) instanceof NullPointerException);
    }
}
