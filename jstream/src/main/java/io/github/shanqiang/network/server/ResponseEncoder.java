package io.github.shanqiang.network.server;

import io.github.shanqiang.network.client.RequestEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponseEncoder extends MessageToByteEncoder<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(ResponseEncoder.class);
    private static final byte[] LENGTH_PLACEHOLDER = new byte[RequestEncoder.LENGTH_FIELD_LENGTH];

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Integer returnValue, ByteBuf byteBuf) throws Exception {
        try {
            int startIdx = byteBuf.writerIndex();
            byteBuf.writeBytes(LENGTH_PLACEHOLDER);
            byteBuf.writeInt(returnValue);
            int endIdx = byteBuf.writerIndex();
            byteBuf.setInt(startIdx, endIdx - startIdx - RequestEncoder.LENGTH_FIELD_LENGTH);
        } catch (Throwable t) {
            logger.error("", t);
            System.exit(-2);
        }
    }
}
