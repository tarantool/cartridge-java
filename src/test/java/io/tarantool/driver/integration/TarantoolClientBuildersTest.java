package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.clientbuilder.TarantoolClusterClientBuilder;
import io.tarantool.driver.clientbuilder.TarantoolProxyClientBuilder;
import io.tarantool.driver.clientbuilder.TarantoolRetryingClientBuilder;
import io.tarantool.driver.clientfactory.TarantoolClientFactory;
import io.tarantool.driver.clientfactory.TarantoolClusterClientBuilderDecorator;
import io.tarantool.driver.clientfactory.TarantoolRetryingClientBuilderDecorator;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TarantoolClientBuildersTest {

    @Test
    void should_builder_createOneInstanceRetryingBuilderForAllSteps() {
        TarantoolRetryingClientBuilder firstStep = RetryingTarantoolTupleClient.builder()
                .withCredentials("admin", "23123");

        TarantoolRetryingClientBuilder secondStep = firstStep
                .withRetryPolicyFactory(TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build());

        TarantoolRetryingClientBuilder thirdStep = secondStep
                .withDecoratedClient(ClusterTarantoolTupleClient.builder().build())
                .withAddress("127.0.0.1", 3301);

        TarantoolRetryingClientBuilder fourthStep = thirdStep
                .withAddress(new TarantoolServerAddress("127.0.0.1", 3301));

        RetryingTarantoolTupleClient client = fourthStep.build();

        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
        assertEquals(fourthStep, firstStep);
        assertEquals(fourthStep, secondStep);
        assertEquals(fourthStep, thirdStep);
    }

    @Test
    void should_builder_createOneInstanceProxyBuilderForAllSteps() {
        TarantoolProxyClientBuilder firstStep = ProxyTarantoolTupleClient.builder()
                .withCredentials("admin", "23123");

        TarantoolProxyClientBuilder secondStep = firstStep
                .withAddress("127.0.0.1", 3301);

        TarantoolProxyClientBuilder thirdStep = secondStep
                .withAddress(new TarantoolServerAddress("127.0.0.1", 3301))
                .withMappingConfig(ProxyOperationsMappingConfig.builder()
                        .withDeleteFunctionName("delete")
                        .build());

        TarantoolProxyClientBuilder fourthStep = thirdStep
                .withDecoratedClient(ClusterTarantoolTupleClient.builder().build());

        ProxyTarantoolTupleClient client = fourthStep.build();

        assertEquals(ProxyTarantoolTupleClient.class, client.getClass());
        assertEquals(thirdStep, firstStep);
        assertEquals(thirdStep, secondStep);
        assertEquals(thirdStep, fourthStep);
    }

    @Test
    void should_builder_createOneInstanceClusterBuilderForAllSteps() {
        TarantoolClusterClientBuilder firstStep = ClusterTarantoolTupleClient.builder()
                .withCredentials("admin", "23123");

        TarantoolClusterClientBuilder secondStep = firstStep
                .withAddress("127.0.0.1", 3301);

        TarantoolClusterClientBuilder thirdStep = secondStep
                .withAddress(new TarantoolServerAddress("127.0.0.1", 3301));

        ClusterTarantoolTupleClient client = thirdStep.build();

        assertEquals(ClusterTarantoolTupleClient.class, client.getClass());
        assertEquals(thirdStep, firstStep);
        assertEquals(thirdStep, secondStep);
    }

    @Test
    void name() {
        TarantoolClusterClientBuilderDecorator decorator = TarantoolClientFactory.getInstance().createClient();

        TarantoolClusterClientBuilderDecorator decorator2 = decorator
                .withAddress(new TarantoolServerAddress("127.0.0.2", 3302));

        ClusterTarantoolTupleClient client = decorator2.build();

        assertNotNull(client);
    }

    @Test
    void name2() {
        TarantoolClusterClientBuilderDecorator decorator = TarantoolClientFactory.getInstance().createClient();

        TarantoolRetryingClientBuilderDecorator decorator2 = decorator
                .withRetryPolicyFactory(TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build());

        TarantoolClusterClientBuilderDecorator decorator3 = decorator2
                .withDecoratedClient(ClusterTarantoolTupleClient.builder().build())
                .withAddress(new TarantoolServerAddress("127.0.0.2", 3302));


        ClusterTarantoolTupleClient client = decorator3.build();

        assertNotNull(client);
    }

    @Test
    void name3() {
        RetryingTarantoolTupleClient client = TarantoolClientFactory.getInstance().createClient()
                .withCredentials("Test", "test")
                .withMappingConfig(ProxyOperationsMappingConfig.builder()
                        .withDeleteFunctionName("delete")
                        .build())
                .withAddress(new TarantoolServerAddress("127.0.0.2", 3302))
                .withRetryPolicyFactory(TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build())
                .build();

        assertNotNull(client);
    }
}
