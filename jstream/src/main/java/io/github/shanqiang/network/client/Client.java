package io.github.shanqiang.network.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.net.ssl.SSLException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@NotThreadSafe
public class Client {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);

    private final SslContext sslCtx;
    private ChannelFuture channelFuture;
    private final ClientHandler clientHandler;
    private final EventLoopGroup group;
    private volatile boolean closed = true;
    private final Duration requestTimeout;
    private final ReentrantLock reentrantLock = new ReentrantLock();
    private final Condition condition = reentrantLock.newCondition();
    private volatile Throwable throwable;

    private class ClientHandler extends ChannelInboundHandlerAdapter {
        private ByteBuf responseByteBuf;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object object) {
            try {
//                reentrantLock.lock();
//                responseByteBuf = (ByteBuf) object;
//                condition.signal();
            } finally {
//                reentrantLock.unlock();
                ReferenceCountUtil.release(object);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("", cause);
            try {
//                reentrantLock.lock();
                throwable = cause;
//                condition.signal();
                ctx.close();

                // 抛出异常由上层close否则会死锁
                // close();
            } finally {
//                reentrantLock.unlock();
            }
        }
    }

    public Client(boolean isSSL, final String host, final int port, Duration requestTimeout) throws SSLException, InterruptedException {
        this.requestTimeout = requestTimeout;
        this.clientHandler = new ClientHandler();

        if (isSSL) {
            sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        group = new NioEventLoopGroup(1
                , new ThreadFactoryBuilder()
                .setNameFormat("client")
                .build());
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            if (sslCtx != null) {
                                p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                            }

                            p.addLast(
                                    new RequestEncoder(),
                                    new ResponseDecoder(),
                                    clientHandler);
                        }
                    });

            channelFuture = b.connect(host, port).sync();
            closed = false;
        } catch (Throwable t) {
            if (null != channelFuture) {
                channelFuture.channel().close().sync();
            }
            if (null != group) {
                group.shutdownGracefully().sync();
            }
            throw new IllegalStateException(t);
        }
    }

    public int asyncRequest(String cmd, Object... args) throws InterruptedException {
        try {
            List<Object> objects = new ArrayList<>(args.length + 1);
            objects.add(cmd);
            for (int i = 0; i < args.length; i++) {
                objects.add(args[i]);
            }
//            reentrantLock.lock();
            if (null != throwable) {
                Throwable tmp = throwable;
                throwable = null;
                throw new RuntimeException(tmp);
            }
            channelFuture.channel().writeAndFlush(objects);
        } finally {
//            reentrantLock.unlock();
        }
        return 0;
    }

    public int request(String cmd, Object... args) throws InterruptedException {
        try {
            reentrantLock.lock();
            List<Object> objects = new ArrayList<>(args.length + 1);
            objects.add(cmd);
            for (int i = 0; i < args.length; i++) {
                objects.add(args[i]);
            }
            channelFuture.channel().writeAndFlush(objects);
            long nanos = condition.awaitNanos(requestTimeout.toNanos());
            if (clientHandler.responseByteBuf != null) {
                int ret = clientHandler.responseByteBuf.readInt();
                clientHandler.responseByteBuf.release();
                clientHandler.responseByteBuf = null;
                return ret;
            }
            if (nanos <= 0) {
                throw new RuntimeException("request timeout");
            }
            throw new IllegalStateException("not timeout but no response");
        } finally {
            reentrantLock.unlock();
            if (null != throwable) {
                Throwable tmp = throwable;
                throwable = null;
                throw new RuntimeException(tmp);
            }
        }
    }

    public void close() {
        try {
            if (closed) {
                return;
            }
            reentrantLock.lock();
            channelFuture.channel().close().sync();
            group.shutdownGracefully().sync();
            closed = true;
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            group.shutdownGracefully();
            logger.error("not closed before finalize");
        }
    }
}
