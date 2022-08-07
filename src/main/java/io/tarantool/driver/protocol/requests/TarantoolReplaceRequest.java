package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import io.tarantool.driver.protocol.TarantoolRequestType;

import java.util.HashMap;
import java.util.Map;

/**
 * Replace request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 *     https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolReplaceRequest extends TarantoolRequest {

    private TarantoolReplaceRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_REPLACE, body);
    }

    /**
     * Tarantool replace request builder
     */
    public static class Builder {

        final Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(2, 1);
        }

        /**
         * Specify tarantool space ID for operation
         * @param spaceId tarantool space ID
         * @return builder
         */
        public TarantoolReplaceRequest.Builder withSpaceId(int spaceId) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode(), spaceId);
            return this;
        }

        /**
         * Specify tuple value
         * @param tuple tuple value
         * @return builder
         */
        public TarantoolReplaceRequest.Builder withTuple(Packable tuple) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_TUPLE.getCode(), tuple);
            return this;
        }

        /**
         * Build a {@link TarantoolReplaceRequest} instance
         * @param mapper configured {@link MessagePackObjectMapper} instance
         * @return instance of replace request
         * @throws TarantoolProtocolException if some required params is missing
         */
        public TarantoolReplaceRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the replace request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_TUPLE.getCode())) {
                throw new TarantoolProtocolException("Tuple value must be specified for the replace request");
            }

            return new TarantoolReplaceRequest(new TarantoolRequestBody(bodyMap, mapper));
        }
    }
}
