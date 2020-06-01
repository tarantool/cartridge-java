package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import io.tarantool.driver.protocol.TarantoolRequestType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Select request. See {@link "https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests"}
 */
public class TarantoolSelectRequest extends TarantoolRequest {

    /**
     * (non-Javadoc)
     */
    private TarantoolSelectRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_SELECT, body);
    }

    /**
     * Tarantool select request builder
     */
    public static class Builder {

        Map<Integer, Object> selectMap;

        public Builder() {
            this.selectMap = new HashMap<>(6, 1);
        }

        public Builder withSpaceId(int spaceId) {
            this.selectMap.put(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode(), spaceId);
            return this;
        }

        public Builder withIndexId(int indexId) {
            this.selectMap.put(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode(), indexId);
            return this;
        }

        public Builder withLimit(long limit) {
            this.selectMap.put(TarantoolRequestFieldType.IPROTO_LIMIT.getCode(), limit);
            return this;
        }

        public Builder withOffset(long offset) {
            this.selectMap.put(TarantoolRequestFieldType.IPROTO_OFFSET.getCode(), offset);
            return this;
        }

        public Builder withIteratorType(TarantoolIteratorType iteratorType) {
            this.selectMap.put(TarantoolRequestFieldType.IPROTO_ITERATOR.getCode(), iteratorType.getCode());
            return this;
        }

        public Builder withKeyValues(List<?> keyValues) {
            this.selectMap.put(TarantoolRequestFieldType.IPROTO_KEY.getCode(), keyValues);
            return this;
        }

        public TarantoolSelectRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!selectMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the select request");
            }
            if (!selectMap.containsKey(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode())) {
                throw new TarantoolProtocolException("Index ID must be specified in the select request");
            }
            if (!selectMap.containsKey(TarantoolRequestFieldType.IPROTO_OFFSET.getCode())) {
                throw new TarantoolProtocolException("Offset must be specified in the select request");
            }
            if (!selectMap.containsKey(TarantoolRequestFieldType.IPROTO_LIMIT.getCode())) {
                throw new TarantoolProtocolException("Limit must be specified in the select request");
            }
            if (!selectMap.containsKey(TarantoolRequestFieldType.IPROTO_ITERATOR.getCode())) {
                throw new TarantoolProtocolException("Iterator type must be specified in the select request");
            }
            if (!selectMap.containsKey(TarantoolRequestFieldType.IPROTO_KEY.getCode())) {
                throw new TarantoolProtocolException("Key values must be specified in the select request");
            }
            return new TarantoolSelectRequest(new TarantoolRequestBody(selectMap, mapper));
        }
    }
}
