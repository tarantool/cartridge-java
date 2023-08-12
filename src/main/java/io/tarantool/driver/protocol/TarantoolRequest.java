package io.tarantool.driver.protocol;

import io.tarantool.driver.exceptions.TarantoolDecoderException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessagePacker;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Base class for all kinds of requests to Tarantool server.
 * See <a href="https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests">
 * https://www.tarantool.io/en/doc/2.3/dev_guide/internals/box_protocol/#binary-protocol-requests</a>
 *
 * @author Alexey Kuzin
 */
public abstract class TarantoolRequest {

    private static final AtomicLong syncId = new AtomicLong(0);
    private static final Supplier<Long> syncIdSupplier =
        () -> syncId.updateAndGet(n -> (n >= Long.MAX_VALUE) ? 1 : n + 1);

    private final TarantoolHeader header;
    private final TarantoolRequestBody body;
    private final Optional<TarantoolRequestSignature> signature;

    /**
     * Basic constructor. Sets an auto-incremented request ID into the Tarantool packet header.
     *
     * @param type      request type code supported by Tarantool
     * @param body      request body, may be empty
     * @param signature request signature, may be null
     * @see TarantoolRequestType
     */
    public TarantoolRequest(TarantoolRequestType type, TarantoolRequestBody body, TarantoolRequestSignature signature) {
        this.header = new TarantoolHeader(syncIdSupplier.get(), type.getCode());
        this.body = body;
        this.signature = Optional.ofNullable(signature);
    }

    /**
     * Get header
     *
     * @return header instance
     */
    public TarantoolHeader getHeader() {
        return header;
    }

    /**
     * Get body
     *
     * @return instance of a {@link Packable}
     */
    public TarantoolRequestBody getBody() {
        return body;
    }

    /**
     * Get signature
     *
     * @return request signature that uniquely represents the operation and argument types
     */
    public Optional<TarantoolRequestSignature> getSignature() {
        return signature;
    }

    /**
     * Encode incapsulated data using {@link MessagePacker}
     *
     * @param packer configured {@link MessagePacker}
     * @param mapper object-to-entity mapper
     * @throws TarantoolDecoderException if encoding failed
     */
    public void toMessagePack(MessagePacker packer, MessagePackObjectMapper mapper)
        throws TarantoolDecoderException {
        try {
            packer.packValue(header.toMessagePackValue(mapper));
            packer.packValue(body.toMessagePackValue(mapper));
        } catch (IOException | MessagePackException e) {
            throw new TarantoolDecoderException(header, e);
        }
    }

    /**
     * Base class for request builder implementations
     */
    protected abstract static class Builder<B extends Builder<B>> {
        protected TarantoolRequestSignature signature;

        protected abstract B self();

        /**
         * Set request signature
         *
         * @param signature request signature
         */
        public B withSignature(TarantoolRequestSignature signature) {
            this.signature = signature;
            return self();
        }
    }
}
