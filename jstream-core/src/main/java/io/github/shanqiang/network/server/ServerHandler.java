package io.github.shanqiang.network.server;

import io.github.shanqiang.exception.UnknownCommandException;
import io.github.shanqiang.sp.Rehash;
import io.github.shanqiang.network.Command;
import io.github.shanqiang.network.LZ4;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.github.shanqiang.network.LZ4.decompress;
import static java.lang.String.format;

public class ServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ServerHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            String clientIp = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress().getHostAddress();

            ByteBuf frame = (ByteBuf) msg;
            if (frame.nioBufferCount() != 1) {
                throw new IllegalArgumentException(format("frame.nioBufferCount: %d", frame.nioBufferCount()));
            }
            String cmd = readString(frame);
            int ret = 0;
            if (cmd.equals(Command.REHASH)) {
                String uniqueName = readString(frame);
                int thread = frame.readInt();
                int restoredSize = frame.readInt();
                ByteBuffer restored = LZ4.decompress(frame.nioBuffer(), restoredSize);
                ret = Rehash.fromOtherServer(uniqueName, thread, restored);
            } else if (cmd.equals(Command.REHASH_FINISHED)) {
                String uniqueName = readString(frame);
                int server = frame.readInt();
                ret = Rehash.otherServerFinished(uniqueName, server);
            } else {
                throw new UnknownCommandException(cmd);
            }
            ctx.write(ret);
        } catch (Throwable t) {
            logger.error("", t);
            ctx.write(-1);
        } finally {
            ReferenceCountUtil.release(msg);
            ctx.flush();
        }
    }

    private String readString(ByteBuf byteBuf) {
        int len = byteBuf.readInt();
        byte[] bytes = new byte[len];
        byteBuf.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!"Connection reset by peer".equalsIgnoreCase(cause.getMessage())) {
            logger.error("", cause);
        }
        ctx.close();
    }
}
