package io.tarantool.driver.codecs;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.tarantool.driver.protocol.TarantoolResponse;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ByteBufferInput;

import java.util.List;
import java.nio.ByteBuffer;

/**
 * Converts Tarantool server responses from MessagePack frames to Java objects
 *
 * @author Alexey Kuzin
 */
public class MessagePackFrameDecoder extends ByteToMessageDecoder {

    private static final int MINIMAL_HEADER_SIZE = 5; // MP_UINT32
    private static final int MINIMAL_BODY_SIZE = 1 * 1024 * 1024; // 1 MB
    private int size;
    private final ByteBuffer lenBuffer = ByteBuffer.allocateDirect(MINIMAL_HEADER_SIZE);
    private final ByteBufferInput lenBufferInput = new ByteBufferInput(lenBuffer);
    private final MessageUnpacker lenUnpacker = new MessagePack.UnpackerConfig().newUnpacker(lenBufferInput);
    private final ByteBuffer bodyBuffer = ByteBuffer.allocateDirect(MINIMAL_BODY_SIZE);
    private final ByteBufferInput bodyBufferInput = new ByteBufferInput(bodyBuffer);
    private final MessageUnpacker bodyUnpacker = new MessagePack.UnpackerConfig().newUnpacker(bodyBufferInput);

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list)
        throws Exception {
        if (byteBuf.readableBytes() < MINIMAL_HEADER_SIZE) {
            return;
        }

        byteBuf.markReaderIndex();
        lenBuffer.clear();
        lenBufferInput.reset(lenBuffer);
        lenUnpacker.reset(lenBufferInput);
        byteBuf.readBytes(lenBuffer);
        size = lenUnpacker.unpackInt();

        if (byteBuf.readableBytes() < size) {
            byteBuf.resetReaderIndex();
            return;
        }

        bodyBuffer.clear();
        bodyBuffer.limit(size);
        bodyBufferInput.reset(bodyBuffer);
        bodyUnpacker.reset(bodyBufferInput);
        byteBuf.readBytes(bodyBuffer);
        list.add(TarantoolResponse.fromMessagePack(bodyUnpacker));
        size = 0;
    }
}
