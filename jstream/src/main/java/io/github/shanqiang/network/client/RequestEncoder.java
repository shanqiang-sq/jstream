package io.github.shanqiang.network.client;

import io.github.shanqiang.exception.UnknownCommandException;
import io.github.shanqiang.table.Table;
import io.github.shanqiang.network.Command;
import io.github.shanqiang.network.LZ4;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RequestEncoder extends MessageToByteEncoder<List<Object>> {
    private static final Logger logger = LoggerFactory.getLogger(RequestEncoder.class);
    public static final int LENGTH_FIELD_LENGTH = 4;
    private static final byte[] LENGTH_PLACEHOLDER = new byte[LENGTH_FIELD_LENGTH];

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, List<Object> objects, ByteBuf byteBuf) {
        try {
            if (null == objects || objects.isEmpty()) {
                throw new IllegalArgumentException();
            }

            byteBuf.writeBytes(LENGTH_PLACEHOLDER);
            int startIdx = byteBuf.writerIndex();

            String cmd = (String) objects.get(0);
            writeString(cmd, byteBuf);
            if (cmd.equals(Command.REHASH)) {
                writeString((String) objects.get(1), byteBuf);

                int thread = (int) objects.get(2);
                byteBuf.writeInt(thread);

                Table table = (Table) objects.get(3);
                byte[] bytes = table.serialize();
                byteBuf.writeInt(bytes.length);
                bytes = LZ4.compress(bytes);
                byteBuf.writeBytes(bytes);
            } else if (cmd.equals(Command.REHASH_FINISHED)) {
                writeString((String) objects.get(1), byteBuf);

                int server = (int) objects.get(2);
                byteBuf.writeInt(server);
            } else {
                throw new UnknownCommandException(cmd);
            }

            int endIdx = byteBuf.writerIndex();
            byteBuf.setInt(startIdx - LENGTH_FIELD_LENGTH, endIdx - startIdx);
        } catch (Throwable t) {
            logger.error("", t);
            System.exit(-2);
        }
    }

    private void writeString(String string, ByteBuf byteBuf) {
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        byteBuf.writeInt(bytes.length);
        byteBuf.writeBytes(bytes);
    }
}
