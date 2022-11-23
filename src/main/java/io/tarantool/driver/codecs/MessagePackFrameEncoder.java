package io.tarantool.driver.codecs;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolRequest;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

/**
 * Converts Tarantool requests from Java objects to MessagePack frames
 *
 * @author Alexey Kuzin
 */
public class MessagePackFrameEncoder extends MessageToByteEncoder<TarantoolRequest> {

    private static final int MINIMAL_HEADER_SIZE = 5; // MP_UINT32
    private final MessagePackObjectMapper mapper;

    public MessagePackFrameEncoder(MessagePackObjectMapper mapper) {
        super();
        this.mapper = mapper;
    }

    @Override
    protected void encode(
        ChannelHandlerContext ctx, TarantoolRequest tarantoolRequest,
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
}
