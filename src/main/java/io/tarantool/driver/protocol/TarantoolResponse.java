package io.tarantool.driver.protocol;

import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.MapValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.util.Iterator;

/**
 * Base class for all kinds of responses received from Tarantool server.
 *
 * See <a href="https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#binary-protocol-responses-if-no-error-and-no-sql">
 *     https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#binary-protocol-responses-if-no-error-and-no-sql</a>
 *
 * @author Alexey Kuzin
 */
public final class TarantoolResponse {
    private final Long syncId;
    private final Long code;
    private final TarantoolResponseBody body;
    private final TarantoolResponseType responseType;

    /**
     * Basic constructor.
     * @param syncId the request ID passed back from Tarantool server
     * @param code the result code returned in response header
     * @param body response body
     * @throws TarantoolProtocolException if the passed body is invalid
     * @see MapValue
     */
    private TarantoolResponse(Long syncId, Long code, TarantoolResponseBody body) throws TarantoolProtocolException {
        TarantoolResponseType responseType = TarantoolResponseType.fromCode(code);
        switch (responseType) {
            case IPROTO_OK:
                switch (body.getResponseBodyType()) {
                    case IPROTO_SQL:
                        throw new UnsupportedOperationException("Tarantool SQL is not supported yet");
                    case IPROTO_ERROR:
                        throw new TarantoolProtocolException(
                                "Response body first key for IPROTO_OK code must be either IPROTO_DATA or IPROTO_SQL");
                }
                break;
            case IPROTO_NOT_OK:
                switch (body.getResponseBodyType()) {
                    case IPROTO_DATA:
                    case IPROTO_SQL:
                        throw new TarantoolProtocolException(
                                "Response body first key for code other from IPROTO_OK must be only IPROTO_ERROR");
                }
        }
        this.responseType = responseType;
        this.syncId = syncId;
        this.code = code;
        this.body = body;
    }

    /**
     * Get request ID
     * @return a number
     */
    public Long getSyncId() {
        return syncId;
    }

    /**
     * Get response body
     * @return a MessagePack entity
     * @see Value
     */
    public TarantoolResponseBody getBody() {
        return body;
    }

    /**
     * Get response type
     * @return code of {@link TarantoolRequestType} type
     */
    public TarantoolResponseType getResponseType() {
        return responseType;
    }

    /**
     * Get response code
     * @return a number, equal to 0 in case of OK response
     * @see TarantoolResponseType
     */
    public Long getResponseCode() {
        return code;
    }

    /**
     * Create Tarantool response from the decoded binary data using {@link MessageUnpacker}
     * @param unpacker configured {@link MessageUnpacker}
     * @param size payload size
     * @return Tarantool response populated from the decoded binary data
     * @throws TarantoolProtocolException if the unpacked data is invalid
     */
    public static TarantoolResponse fromMessagePack(MessageUnpacker unpacker, int size)
            throws TarantoolProtocolException {
        try {
            TarantoolHeader header = TarantoolHeader.fromMessagePackValue(unpacker.unpackValue());
            TarantoolResponseBody responseBody = new EmptyTarantoolResponseBody();
            if (unpacker.getTotalReadBytes() < size) {
                Value bodyMap = unpacker.unpackValue();
                if (!bodyMap.isMapValue()) {
                    throw new TarantoolProtocolException("Response body must be of MP_MAP type");
                }
                MapValue values = bodyMap.asMapValue();
                Iterator<Value> it = values.keySet().iterator();
                if (it.hasNext()) {
                    Value key = it.next();
                    if (!key.isIntegerValue()) {
                        throw new TarantoolProtocolException("Response body first key must be of MP_INT type");
                    }
                    responseBody = new NotEmptyTarantoolResponseBody(
                            key.asIntegerValue().asInt(), values.map().get(key));
                }
            }
            return new TarantoolResponse(header.getSync(), header.getCode(), responseBody);
        } catch (IOException e) {
            throw new TarantoolProtocolException(e);
        }
    }
}
