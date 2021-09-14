package io.tarantool.driver.api.client;

public interface TarantoolClientBuilderFirstStep {

    TarantoolClientBuilderSecondStep withDefaultCredentials();

    TarantoolClientBuilderSecondStep withCredentials(String user, String password);

}
