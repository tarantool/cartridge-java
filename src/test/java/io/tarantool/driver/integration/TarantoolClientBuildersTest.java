package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.clientbuilder.TarantoolClusterClientBuilder;
import io.tarantool.driver.clientbuilder.TarantoolProxyClientBuilder;
import io.tarantool.driver.clientbuilder.TarantoolRetryingClientBuilder;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TarantoolClientBuildersTest {

    @Test
    void should_builder_createOneInstanceRetryingBuilderForAllSteps() {
        TarantoolRetryingClientBuilder firstStep = RetryingTarantoolTupleClient.builder()
                .withCredentials("admin", "23123");

        TarantoolRetryingClientBuilder secondStep = firstStep
                .withRetryPolicyFactory(TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build());

        TarantoolRetryingClientBuilder thirdStep = secondStep
                .withAddress("127.0.0.1", 3301);

        TarantoolRetryingClientBuilder fourthStep = thirdStep
                .withAddress(new TarantoolServerAddress("127.0.0.1", 3301));

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = fourthStep.build();

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
}
