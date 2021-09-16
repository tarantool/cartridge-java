package io.tarantool.driver.api.client;

import io.tarantool.driver.auth.SimpleTarantoolCredentials;

public class ClientCredentialsParameter extends AbstractClientParameter<SimpleTarantoolCredentials> {

    public ClientCredentialsParameter(SimpleTarantoolCredentials tarantoolCredentials) {
        super(tarantoolCredentials);
    }

    @Override
    public ClientParameterType getParameterType() {
        return ClientParameterType.CREDENTIALS;
    }

}
