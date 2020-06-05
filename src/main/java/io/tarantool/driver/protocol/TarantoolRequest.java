package io.tarantool.driver.protocol;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.core.MessagePacker;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Base class for all kinds of requests to Tarantool server. See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequest {

    private static AtomicLong syncId = new AtomicLong(0);
    private static Supplier<Long> syncIdSupplier = () -> syncId.updateAndGet(n -> (n >= Long.MAX_VALUE) ? 1 : n + 1);

    private TarantoolHeader header;
    private TarantoolRequestBody body;

    /**
     * Basic constructor. Sets an auto-incremented request ID into the Tarantool packet header.
     * @param type request type code supported by Tarantool
     * @param body request body, may be empty
     * @see TarantoolRequestType
     */
    public TarantoolRequest(TarantoolRequestType type, TarantoolRequestBody body) {
        this.header = new TarantoolHeader(syncIdSupplier.get(), type.getCode());
        this.body = body;
    }

    /**
     * Get header
     * @return header instance
     */
    public TarantoolHeader getHeader() {
        return header;
    }

    /**
     * Get body
     * @return instance of a {@link Packable}
     */
    public Packable getBody() {
        return body;
    }

    /**
     * Encode incapsulated data using {@link MessagePacker}
     * @param packer configured {@link MessagePacker}
     * @param mapper object-to-entity mapper
     * @throws Exception if encoding failed
     */
    public void toMessagePack(MessagePacker packer, MessagePackObjectMapper mapper) throws Exception {
        packer.packValue(header.toMessagePackValue(mapper));
        packer.packValue(body.toMessagePackValue(mapper));
    }
}
