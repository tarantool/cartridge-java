package io.tarantool.driver.codecs;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolResponse;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

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
    protected void encode(ChannelHandlerContext channelHandlerContext, TarantoolRequest tarantoolRequest,
                          ByteBuf byteBuf) throws Exception {
        if (byteBuf.writableBytes() >= MINIMAL_PACKET_SIZE) {
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            tarantoolRequest.toMessagePack(packer, mapper);
            long outputSize = packer.getTotalWrittenBytes();
            byte[] output = packer.toByteArray();
            packer.close();
            packer = MessagePack.newDefaultBufferPacker();
            packer.packLong(outputSize);
            byteBuf.writeBytes(packer.toByteArray());
            packer.close();
            byteBuf.writeBytes(output);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf,
                          List<Object> list) throws Exception {
        if (byteBuf.readableBytes() > MINIMAL_HEADER_SIZE) {
            byteBuf.markReaderIndex();
            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(byteBuf.nioBuffer(0, MINIMAL_HEADER_SIZE));
            int size = unpacker.unpackInt();
            unpacker.close();
            if (byteBuf.readableBytes() >= size) {
                unpacker = MessagePack.newDefaultUnpacker(byteBuf.nioBuffer(0, size));
                list.add(TarantoolResponse.fromMessagePack(unpacker));
            } else {
                byteBuf.resetReaderIndex();
            }
        }
    }
}
