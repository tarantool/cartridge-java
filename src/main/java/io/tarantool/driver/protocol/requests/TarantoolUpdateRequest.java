package io.tarantool.driver.protocol.requests;


import io.tarantool.driver.api.tuple.operations.TupleOperation;
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
 * Update request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 * https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolUpdateRequest extends TarantoolRequest {

    /**
     * (non-Javadoc)
     */
    private TarantoolUpdateRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_UPDATE, body);
    }

    /**
     * Tarantool update request builder
     */
    public static class Builder {

        Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(4, 1);
        }

        public Builder withSpaceId(int spaceId) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode(), spaceId);
            return this;
        }

        public Builder withIndexId(int indexId) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode(), indexId);
            return this;
        }

        public Builder withKeyValues(List<?> keyValues) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_KEY.getCode(), keyValues);
            return this;
        }

        public Builder withTupleOperations(List<TupleOperation> operations) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_TUPLE.getCode(), operations);
            return this;
        }

        public TarantoolUpdateRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the update request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode())) {
                throw new TarantoolProtocolException("Index ID must be specified in the update request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_KEY.getCode())) {
                throw new TarantoolProtocolException("Key values must be specified in the update request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_TUPLE.getCode())) {
                throw new TarantoolProtocolException("Update operations must be specified for the update request");
            }

            return new TarantoolUpdateRequest(new TarantoolRequestBody(bodyMap, mapper));
        }
    }
}
