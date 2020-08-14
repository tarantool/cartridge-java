package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import io.tarantool.driver.protocol.TarantoolRequestType;
import io.tarantool.driver.protocol.operations.TupleOperations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Upsert request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 *     https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolUpsertRequest extends TarantoolRequest {

    /**
     * (non-Javadoc)
     */
    private TarantoolUpsertRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_UPSERT, body);
    }

    /**
     * Tarantool update request builder
     */
    public static class Builder {

        Map<Integer, Object> bodyMap;
        TarantoolSpaceMetadata metadata;

        public Builder(TarantoolSpaceMetadata metadata) {
            this.bodyMap = new HashMap<>(5, 1);
            this.metadata = metadata;
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

        public Builder withTuple(TarantoolTuple tuple) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_TUPLE.getCode(), tuple.getFields());
            return this;
        }

        public Builder withTupleOperations(TupleOperations operations) {
            operations.asList().forEach(op -> {
                if (op.getFieldNumber() == null) {
                    op.setFieldNumber(metadata.getFieldPositionByName(op.getFieldName()));
                }
            });
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_OPS.getCode(), operations.asList());
            return this;
        }

        public TarantoolUpsertRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the upsert request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_INDEX_ID.getCode())) {
                throw new TarantoolProtocolException("Index ID must be specified in the upsert request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_KEY.getCode())) {
                throw new TarantoolProtocolException("Key values must be specified in the upsert request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_TUPLE.getCode())) {
                throw new TarantoolProtocolException("Tuple value must be specified for the upsert request");
            }
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_OPS.getCode())) {
                throw new TarantoolProtocolException("Update operations must be specified for the upsert request");
            }

            return new TarantoolUpsertRequest(new TarantoolRequestBody(bodyMap, mapper));
        }
    }
}
