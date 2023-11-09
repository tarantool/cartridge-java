package io.tarantool.driver.codecs;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolRequest;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.ArrayBufferOutput;
import org.msgpack.core.buffer.MessageBuffer;

/**
 * Converts Tarantool requests from Java objects to MessagePack frames
 *
 * @author Alexey Kuzin
 */
public class MessagePackFrameEncoder extends MessageToByteEncoder<TarantoolRequest> {

    private static final int MINIMAL_HEADER_SIZE = 8; // MP_UINT32
    private static final int MINIMAL_BODY_SIZE = 1 * 1024 * 1024; // 1 MB
    private final MessagePackObjectMapper mapper;
    private final ArrayBufferOutput lenBufferOutput = new ArrayBufferOutput(MINIMAL_HEADER_SIZE);
    private final MessagePacker lenPacker = new MessagePack.PackerConfig().newPacker(lenBufferOutput);
    private final ArrayBufferOutput bodyBufferOutput = new ArrayBufferOutput(MINIMAL_BODY_SIZE);
    private final MessagePacker bodyPacker = new MessagePack.PackerConfig().newPacker(bodyBufferOutput);

    public MessagePackFrameEncoder(MessagePackObjectMapper mapper) {
        super();
        this.mapper = mapper;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, TarantoolRequest tarantoolRequest, ByteBuf byteBuf)
        throws Exception {
        bodyBufferOutput.clear();
        bodyPacker.clear();
        tarantoolRequest.toMessagePack(bodyPacker, mapper);
        bodyPacker.flush();
        MessageBuffer bodyBuffer = bodyBufferOutput.toMessageBuffer();
        lenBufferOutput.clear();
        lenPacker.clear();
        lenPacker.packLong(bodyBuffer.size());
        lenPacker.flush();
        MessageBuffer lenBuffer = lenBufferOutput.toMessageBuffer();
        byteBuf.capacity(bodyBuffer.size() + lenBuffer.size());
        byteBuf.writeBytes(
            lenBufferOutput.toMessageBuffer().sliceAsByteBuffer(0, lenBuffer.size()));
        byteBuf.writeBytes(
            bodyBufferOutput.toMessageBuffer().sliceAsByteBuffer(0, bodyBuffer.size()));
    }
}
