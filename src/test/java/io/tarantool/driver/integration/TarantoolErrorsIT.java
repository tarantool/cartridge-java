package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.exceptions.TarantoolAccessDeniedException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.exceptions.TarantoolInternalNetworkException;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TarantoolErrorsIT extends SharedCartridgeContainer {

    public static String USER_NAME;
    public static String RESTRICTED_USER = "restricted_user";
    public static String PASSWORD;
    public static String RESTRICTED_PASSWORD = "restricted_secret";

    @BeforeAll
    public static void setUp() {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
    }

    private ProxyTarantoolTupleClient setupClient(String userName, String password) {
        TarantoolClientConfig config = TarantoolClientConfig.builder()
            .withCredentials(new SimpleTarantoolCredentials(userName, password))
            .withConnectTimeout(1000)
            .withReadTimeout(1000)
            .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
            config, container.getRouterHost(), container.getRouterPort());
        return new ProxyTarantoolTupleClient(clusterClient);
    }

    private ProxyTarantoolTupleClient setupAdminClient() {
        return setupClient(USER_NAME, PASSWORD);
    }

    private ProxyTarantoolTupleClient setupRestrictedClient() {
        return setupClient(RESTRICTED_USER, RESTRICTED_PASSWORD);
    }

    @Test
    void testNetworkError_boxErrorUnpackNoConnection() {
        try {
            ProxyTarantoolTupleClient client = setupAdminClient();

            client.callForSingleResult("box_error_unpack_no_connection", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 77"));
            assertTrue(message.contains("message: Connection is not established"));
            assertTrue(message.contains("type: ClientError"));
            assertTrue(message.contains("trace:"));
        }
    }

    @Test
    void testNetworkError_boxErrorUnpackTimeout() {
        try {
            ProxyTarantoolTupleClient client = setupAdminClient();

            client.callForMultiResult("box_error_unpack_timeout",
                Collections.singletonList("Some error"),
                ArrayList::new,
                String.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 78"));
            assertTrue(message.contains("message: Timeout exceeded"));
            assertTrue(message.contains("type: ClientError"));
            assertTrue(message.contains("trace:"));
        }
    }

    @Test
    void testNetworkError_boxErrorTimeout() {
        try {
            ProxyTarantoolTupleClient client = setupAdminClient();

            client.callForMultiResult("box_error_timeout",
                Collections.singletonList("Some error"),
                ArrayList::new,
                String.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 78"));
            assertTrue(message.contains("message: Timeout exceeded"));
        }
    }

    @Test
    void testNetworkError_crudErrorTimeout() {
        try {
            ProxyTarantoolTupleClient client = setupAdminClient();

            client.callForSingleResult("crud_error_timeout", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("\"code\":78"));
            assertTrue(message.contains("\"type\":\"ClientError\""));
            assertTrue(message.contains("\"message\":\"Timeout exceeded\""));
            assertTrue(message.contains("\"trace\":"));
        }
    }

    @Test
    void testNonNetworkError_boxErrorUnpack() {
        try {
            ProxyTarantoolTupleClient client = setupAdminClient();

            client.callForSingleResult("box_error_non_network_error", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalException);
            assertFalse(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("code: 40"));
            assertTrue(message.contains("message: Failed to write to disk"));
            assertTrue(message.contains("type: ClientError"));
            assertTrue(message.contains("trace:"));
        }
    }

    @Test
    void testNonNetworkError_boxError() {
        try {
            ProxyTarantoolTupleClient client = setupAdminClient();

            client.callForSingleResult("raising_error", HashMap.class).get();
            fail("Exception must be thrown after last retry attempt.");
        } catch (Throwable e) {
            String message = e.getCause().getMessage();

            assertTrue(e.getCause() instanceof TarantoolInternalException);
            assertFalse(e.getCause() instanceof TarantoolInternalNetworkException);
            assertTrue(message.contains("InnerErrorMessage:"));
            assertTrue(message.contains("code: 32"));
            assertTrue(message.contains("message:"));
        }
    }

    @Test
    void test_should_throwTarantoolNoSuchProcedureException_ifProcedureIsNil() {
        ProxyTarantoolTupleClient client = setupAdminClient();

        client.eval("rawset(_G, 'crud', nil)").join();

        CompletionException exception =
            assertThrows(CompletionException.class,
                () -> client.space("test_space").select(Conditions.any()).join());

        assertTrue(exception.getCause() instanceof TarantoolNoSuchProcedureException);
        assertTrue(exception.getCause().getMessage().contains("Procedure 'crud.select' is not defined"));

        client.eval("rawset(_G, 'crud', require('crud'))").join();

        assertDoesNotThrow(() -> client.space("test_space").select(Conditions.any()).join());
    }

    @Test
    void test_should_throwTarantoolAccessDenied_ifUserHasRestrictions() {
        ProxyTarantoolTupleClient adminClient = setupAdminClient();
        ProxyTarantoolTupleClient restrictedClient = setupRestrictedClient();

        // Both users have access to "returning_number" function
        assertEquals(
            2,
            adminClient.callForSingleResult("returning_number", Integer.class).join());
        assertEquals(
            2,
            restrictedClient.callForSingleResult("returning_number", Integer.class).join());

        // Only admin user has access to "ddl.get_schema"
        assertDoesNotThrow(() -> adminClient.metadata().getSpaceByName("test_space"));
        TarantoolClientException exception = assertThrows(
            TarantoolClientException.class,
            () -> restrictedClient.metadata().getSpaceByName("test_space"));
        Throwable cause = exception.getCause();
        assertTrue(cause instanceof TarantoolAccessDeniedException);
        assertTrue(cause.getMessage()
            .contains("Execute access to function 'ddl.get_schema' is denied for user 'restricted_user'"));
    }
}
