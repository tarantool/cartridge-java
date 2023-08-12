package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolIteratorType;
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
 * Select request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 * https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>,
 * <a href="https://www.tarantool.io/en/doc/2.3/reference/reference_lua/box_index/#box-index-select">
 * https://www.tarantool.io/en/doc/2.3/reference/reference_lua/box_index/#box-index-select</a>
 */
public final class TarantoolSelectRequest extends TarantoolRequest {

    private TarantoolSelectRequest(TarantoolRequestBody body, TarantoolRequestSignature signature) {
        super(TarantoolRequestType.IPROTO_SELECT, body, signature);
    }

    /**
     * Tarantool select request builder
     */
    public static class Builder extends TarantoolRequest.Builder<Builder> {

        Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(6, 1);
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
         * Specify the maximum number of tuples returned by the request.
         *
         * @param limit number
         * @return builder
         */
        public Builder withLimit(long limit) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_LIMIT.getCode(), limit);
            return this;
        }

        /**
         * Specify the offset of the first tuple returned by the request
         *
         * @param offset number
         * @return builder
         */
        public Builder withOffset(long offset) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_OFFSET.getCode(), offset);
            return this;
        }

        /**
         * Specify iterator type
         *
         * @param iteratorType iterator type
         * @return builder
         */
        public Builder withIteratorType(TarantoolIteratorType iteratorType) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_ITERATOR.getCode(), iteratorType.getCode());
            return this;
        }

        /**
         * Specify key values to be matched against the index key
         *
         * @param keyValues key value
         * @return builder
         */
        public Builder withKeyValues(List<?> keyValues) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_KEY.getCode(), keyValues);
            return this;
        }

        /**
         * Build a {@link TarantoolSelectRequest} instance
         *
         * @param mapper configured {@link MessagePackObjectMapper} instance
         * @return instance of select request
         * @throws TarantoolProtocolException if some required params is missing
         */
        public TarantoolSelectRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the select request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode())) {
                throw new TarantoolProtocolException("Index ID must be specified in the select request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_OFFSET.getCode())) {
                throw new TarantoolProtocolException("Offset must be specified in the select request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_LIMIT.getCode())) {
                throw new TarantoolProtocolException("Limit must be specified in the select request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_ITERATOR.getCode())) {
                throw new TarantoolProtocolException("Iterator type must be specified in the select request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_KEY.getCode())) {
                throw new TarantoolProtocolException("Key values must be specified in the select request");
            }
            return new TarantoolSelectRequest(new TarantoolRequestBody(bodyMap, mapper), signature);
        }
    }
}
