package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.api.tuple.operations.TupleOperation;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;
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
 * Upsert request.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 * https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolUpsertRequest extends TarantoolRequest {

    /**
     * (non-Javadoc)
     */
    private TarantoolUpsertRequest(TarantoolRequestBody body, TarantoolRequestSignature signature) {
        super(TarantoolRequestType.IPROTO_UPSERT, body, signature);
    }

    /**
     * Tarantool update request builder
     */
    public static class Builder extends TarantoolRequest.Builder<Builder> {

        Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(4, 1);
        }

        @Override
        protected Builder self() {
            return this;
        }

        public Builder withSpaceId(int spaceId) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode(), spaceId);
            return this;
        }

        public Builder withKeyValues(List<?> keyValues) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_KEY.getCode(), keyValues);
            return this;
        }

        public Builder withTuple(Packable tuple) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_TUPLE.getCode(), tuple);
            return this;
        }

        public Builder withTupleOperations(List<TupleOperation> operations) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_OPS.getCode(), operations);
            return this;
        }

        public TarantoolUpsertRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_SPACE_ID.getCode())) {
                throw new TarantoolProtocolException("Space ID must be specified in the upsert request");
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

            return new TarantoolUpsertRequest(new TarantoolRequestBody(bodyMap, mapper), signature);
        }
    }
}
