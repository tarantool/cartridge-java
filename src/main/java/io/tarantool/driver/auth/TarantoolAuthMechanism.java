package io.tarantool.driver.auth;

/**
 * Provides signatures for the authentication mechanisms supported by Tarantool
 *
 * @author Alexey Kuzin
 */
public enum TarantoolAuthMechanism {
    CHAPSHA1("chap-sha1");

    private String signature;

    TarantoolAuthMechanism(String signature) {
        this.signature = signature;
    }

    public String getSignature() {
        return signature;
    }
}
