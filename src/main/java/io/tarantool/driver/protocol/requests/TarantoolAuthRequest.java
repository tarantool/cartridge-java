package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.auth.TarantoolAuthMechanism;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestType;
import io.tarantool.driver.util.Assert;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Authentication request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-authentication">
 *     https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-authentication</a>
 *
 * @author Alexey Kuzin
 */
public final class TarantoolAuthRequest extends TarantoolRequest {

    private static final int IPROTO_USER_NAME = 0x23;
    private static final int IPROTO_AUTH_DATA = 0x21;

    private TarantoolAuthRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_AUTH, body);
    }

    /**
     * Tarantool authentication request builder
     */
    public static class Builder {

        private Map<Integer, Object> authMap;

        /**
         * Basic constructor.
         */
        public Builder() {
            authMap = new HashMap<>(2, 1);
        }

        public Builder withUsername(String username) {
            Assert.hasText(username, "Username must not be empty");

            authMap.put(IPROTO_USER_NAME, username);
            return this;
        }

        public Builder withAuthData(TarantoolAuthMechanism authMechanism, byte[] authData) {
            Assert.notNull(authData, "Username must not be empty");

            authMap.put(IPROTO_AUTH_DATA, Arrays.asList(authMechanism.getSignature(), authData));
            return this;
        }

        public TarantoolAuthRequest build() throws TarantoolProtocolException {
            if (authMap.size() < 2) {
                throw new TarantoolProtocolException(
                        "Username and auth data must be specified for Tarantool auth request");
            }
            MessagePackObjectMapper mapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
            return new TarantoolAuthRequest(new TarantoolRequestBody(authMap, mapper));
        }
    }
}
