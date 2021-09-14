package io.tarantool.driver.api.client;

import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.function.UnaryOperator;

public interface TarantoolClientBuilderThirdStep extends TarantoolClientBuilderCompletable {

    TarantoolClientBuilderFourthStep withDefaultCrudMethods();

    TarantoolClientBuilderFourthStep withMappedCrudMethods(UnaryOperator<ProxyOperationsMappingConfig.Builder>
                                                                   mappingConfigBuilder);
}
