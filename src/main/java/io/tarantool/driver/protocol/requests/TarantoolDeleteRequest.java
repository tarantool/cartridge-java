package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import io.tarantool.driver.protocol.TarantoolRequestSignature;
import io.tarantool.driver.protocol.TarantoolRequestType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Delete request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 * https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 * <a href="https://www.tarantool.io/en/doc/2.3/reference/reference_lua/box_index/#box-index-delete">
 * https://www.tarantool.io/en/doc/2.3/reference/reference_lua/box_index/#box-index-delete</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolDeleteRequest extends TarantoolRequest {

    private TarantoolDeleteRequest(TarantoolRequestBody body, TarantoolRequestSignature signature) {
        super(TarantoolRequestType.IPROTO_DELETE, body, signature);
    }

    /**
     * Tarantool delete request builder
     */
    public static class Builder extends TarantoolRequest.Builder<Builder> {

        Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(3, 1);
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Specify tarantool space ID for operation
         *
         * @param spaceId tarantool space ID
         * @return builder
         */
        public Builder withSpaceId(int spaceId) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode(), spaceId);
            return this;
        }

        /**
         * Specify tarantool index ID for operation
         *
         * @param indexId tarantool index ID
         * @return builder
         */
        public Builder withIndexId(int indexId) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode(), indexId);
            return this;
        }

        /**
         * Specify values to be matched against the index key
         *
         * @param keyValues key value
         * @return builder
         */
        public Builder withKeyValues(List<?> keyValues) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_KEY.getCode(), keyValues);
            return this;
        }

        /**
         * Build a {@link TarantoolDeleteRequest} instance
         *
         * @param mapper configured {@link MessagePackObjectMapper} instance
         * @return instance of delete request
         * @throws TarantoolProtocolException if some required params is missing
         */
        public TarantoolDeleteRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the delete request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode())) {
                throw new TarantoolProtocolException("Index ID must be specified in the delete request");
            }
            if ((Integer) bodyMap.get(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode())
                != TarantoolIndexQuery.PRIMARY) {
                throw new TarantoolProtocolException("A delete request can only be executed for the primary key");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_KEY.getCode())) {
                throw new TarantoolProtocolException("Key values must be specified in the delete request");
            }

            return new TarantoolDeleteRequest(new TarantoolRequestBody(bodyMap, mapper), signature);
        }
    }
}
