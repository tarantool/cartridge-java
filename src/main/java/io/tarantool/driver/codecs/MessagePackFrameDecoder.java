package io.tarantool.driver.codecs;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.tarantool.driver.protocol.TarantoolResponse;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.util.List;

/**
 * Converts Tarantool server responses from MessagePack frames to Java objects
 *
 * @author Alexey Kuzin
 */
public class MessagePackFrameDecoder extends ReplayingDecoder<MessagePackFrameDecoder.DecoderState> {

    private static final int MINIMAL_HEADER_SIZE = 5; // MP_UINT32
    private int size;

    public MessagePackFrameDecoder() {
        super(DecoderState.LENGTH);
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list)
            throws Exception {

            switch (state()) {
                case LENGTH:
                    ByteBuf lenBuf = byteBuf.readBytes(MINIMAL_HEADER_SIZE);
                    try (ByteBufInputStream in = new ByteBufInputStream(lenBuf)) {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(in);
                        size = unpacker.unpackInt();
                        unpacker.close();
                        checkpoint(DecoderState.BODY);
                    }
                    lenBuf.release();
                    // TODO no break; and no comment if the fall through is intentional
                case BODY:
                    if (size > 0) {
                        if (byteBuf.readableBytes() < size) {
                            return;
                        }
                        ByteBuf bodyBuf = byteBuf.readBytes(size);
                        try (ByteBufInputStream in = new ByteBufInputStream(bodyBuf)) {
                            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(in);
                            list.add(TarantoolResponse.fromMessagePack(unpacker));
                            unpacker.close();
                            size = 0;
                        }
                        bodyBuf.release();
                    }
                    checkpoint(DecoderState.LENGTH);
                    break;
                default:
                   throw new Error("Shouldn't reach here.");
            }
    }

    protected enum DecoderState {
        LENGTH,
        BODY
    }
}
