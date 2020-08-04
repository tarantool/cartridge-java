package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import io.tarantool.driver.protocol.TarantoolRequestType;

import java.util.HashMap;
import java.util.Map;

/**
 * Insert request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 *     https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolInsertRequest extends TarantoolRequest {

    private TarantoolInsertRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_INSERT, body);
    }

    /**
     * Tarantool insert request builder
     */
    public static class Builder {

        Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(2, 1);
        }

        /**
         * Specify tarantool space ID for operation
         * @param spaceId tarantool space ID
         * @return builder
         */
        public TarantoolInsertRequest.Builder withSpaceId(int spaceId) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode(), spaceId);
            return this;
        }

        /**
         * Specify tuple value
         * @param tuple data which will be insert into space
         * @return builder
         */
        public TarantoolInsertRequest.Builder withTuple(TarantoolTuple tuple) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_TUPLE.getCode(), tuple.getFields());
            return this;
        }

        /**
         * Build a {@link TarantoolInsertRequest} instance
         * @param mapper configured {@link MessagePackObjectMapper} instance
         * @return instance of insert request
         * @throws TarantoolProtocolException if some required params is missing
         */
        public TarantoolInsertRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the insert request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_TUPLE.getCode())) {
                throw new TarantoolProtocolException("Tuple value must be specified for the insert request");
            }

            return new TarantoolInsertRequest(new TarantoolRequestBody(bodyMap, mapper));
        }
    }
}
