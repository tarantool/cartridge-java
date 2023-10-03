package io.tarantool.driver.protocol.requests;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestBody;
import io.tarantool.driver.protocol.TarantoolRequestFieldType;
import io.tarantool.driver.protocol.TarantoolRequestType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Eval request.
 * See <a href="https://www.tarantool.io/latest/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 * https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Sergey Volgin
 */
public final class TarantoolEvalRequest extends TarantoolRequest {

    private TarantoolEvalRequest(TarantoolRequestBody body) {
        super(TarantoolRequestType.IPROTO_EVAL, body);
    }

    /**
     * Tarantool eval request builder
     */
    public static class Builder {

        Map<Integer, Object> bodyMap;

        public Builder() {
            this.bodyMap = new HashMap<>(2, 1);
        }

        /**
         * Specify lua expression
         *
         * @param expression lua expression
         * @return builder
         */
        public Builder withExpression(String expression) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_EXPRESSION.getCode(), expression);
            return this;
        }

        /**
         * Specify eval arguments
         *
         * @param arguments eval arguments
         * @return builder
         */
        public Builder withArguments(Collection<?> arguments) {
            this.bodyMap.put(TarantoolRequestFieldType.IPROTO_TUPLE.getCode(), arguments);
            return this;
        }

        /**
         * Build a {@link TarantoolEvalRequest} instance
         *
         * @param mapper configured {@link MessagePackObjectMapper} instance
         * @return instance of eval request
         * @throws TarantoolProtocolException if some required params is missing
         */
        public TarantoolEvalRequest build(MessagePackObjectMapper mapper) throws TarantoolProtocolException {
            if (!bodyMap.containsKey(TarantoolRequestFieldType.IPROTO_EXPRESSION.getCode())) {
                throw new TarantoolProtocolException("Lua expression must be specified in the eval request");
            }

            return new TarantoolEvalRequest(new TarantoolRequestBody(bodyMap, mapper));
        }
    }
}
