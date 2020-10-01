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
                    try (ByteBufInputStream in = new ByteBufInputStream(byteBuf.readBytes(MINIMAL_HEADER_SIZE))) {
                        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(in);
                        size = unpacker.unpackInt();
                        checkpoint(DecoderState.BODY);
                    }
                case BODY:
                    if (size > 0) {
                        try (ByteBufInputStream in = new ByteBufInputStream(byteBuf.readBytes(size))) {
                            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(in);
                            list.add(TarantoolResponse.fromMessagePack(unpacker));
                            size = 0;
                        }
                    }
                    checkpoint(DecoderState.LENGTH);
            }
    }

    protected enum DecoderState {
        LENGTH,
        BODY
    }
}
