package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import io.tarantool.driver.protocol.TarantoolRequestType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Call request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 *     https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolCallRequest extends TarantoolRequest {

    private TarantoolCallRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_CALL, body);
    }

    /**
     * Tarantool call request builder
     */
    public static class Builder {

        final Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(2, 1);
        }

        /**
         * Specify function name
         * @param functionName function name
         * @return builder
         */
        public Builder withFunctionName(String functionName) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_FUNCTION_NAME.getCode(), functionName);
            return this;
        }

        /**
         * Specify function arguments
         * @param arguments function arguments
         * @return builder
         */
        public Builder withArguments(List<?> arguments) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_TUPLE.getCode(), arguments);
            return this;
        }

        /**
         * Build a {@link TarantoolCallRequest} instance
         * @param mapper configured {@link MessagePackObjectMapper} instance
         * @return instance of call request
         * @throws TarantoolProtocolException if some required params is missing
         */
        public TarantoolCallRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_FUNCTION_NAME.getCode())) {
                throw new TarantoolProtocolException("Function name must be specified in the call request");
            }

            return new TarantoolCallRequest(new TarantoolRequestBody(bodyMap, mapper));
        }
    }
}
