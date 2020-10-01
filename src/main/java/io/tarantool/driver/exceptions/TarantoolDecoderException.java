package io.tarantool.driver.exceptions;

import io.tarantool.driver.protocol.TarantoolHeader;
import io.tarantool.driver.protocol.TarantoolProtocolException;

/**
 * Used in cases when a request or a response body cannot be transformed from/into MessagePack
 *
 * @author Alexey Kuzin
 */
public class TarantoolDecoderException extends TarantoolProtocolException {

    private final TarantoolHeader header;

    public TarantoolDecoderException(TarantoolHeader header, Exception e) {
        super(e);
        this.header = header;
    }

    public TarantoolHeader getHeader() {
        return header;
    }
}
