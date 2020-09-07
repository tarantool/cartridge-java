package io.tarantool.driver.codecs;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.netty.handler.codec.DecoderException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolResponse;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.IOException;
import java.util.List;

/**
 * Converts Tarantool requests and responses from Java objects to bytes and vice versa
 *
 * @author Alexey Kuzin
 */
public class MessagePackFrameCodec extends ByteToMessageCodec<TarantoolRequest> {

    private static final int MINIMAL_HEADER_SIZE = 5; // MP_UINT32
    private static final int MINIMAL_PACKET_SIZE = 4096;
    private MessagePackObjectMapper mapper;

    public MessagePackFrameCodec(MessagePackObjectMapper mapper) {
        super();
        this.mapper = mapper;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, TarantoolRequest tarantoolRequest,
                          ByteBuf byteBuf) throws Exception {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        tarantoolRequest.toMessagePack(packer, mapper);
        long outputSize = packer.getTotalWrittenBytes();
        byteBuf.capacity((int) (outputSize + MINIMAL_HEADER_SIZE));
        byte[] output = packer.toByteArray();
        packer.clear();
        packer.packLong(outputSize);
        byteBuf.writeBytes(packer.toByteArray());
        packer.close();
        byteBuf.writeBytes(output);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf,
                          List<Object> list) throws IOException, TarantoolProtocolException {
        if (byteBuf.readableBytes() > MINIMAL_HEADER_SIZE) {
            byteBuf.markReaderIndex();
            try (ByteBufInputStream in = new ByteBufInputStream(byteBuf)) {
                MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(in);
                int size = unpacker.unpackInt();
                if (size > 0) {
                    byteBuf.readerIndex(MINIMAL_HEADER_SIZE);
                    if (byteBuf.readableBytes() >= size) {
                        int payloadSize = MINIMAL_HEADER_SIZE + size;
                        list.add(TarantoolResponse.fromMessagePack(unpacker, payloadSize));
                        byteBuf.readerIndex(payloadSize);
                        byteBuf.discardReadBytes();
                    } else {
                        byteBuf.resetReaderIndex();
                    }
                }
            } catch (IOException | TarantoolProtocolException | DecoderException e) {
                throw e;
            }
        }
    }
}
