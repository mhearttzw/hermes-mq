package com.echelon.hermes.remoting;

import com.echelon.hermes.remoting.protocol.CommandDecoder;
import com.echelon.hermes.remoting.protocol.CommandEncoder;
import com.echelon.hermes.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：网络服务客户端
 *
 * @author jorelwang
 * @create 2025-09-18 23:10
 */
public class NettyRemotingClient {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingClient.class);

    private final Bootstrap bootstrap;
    private final EventLoopGroup worderGroup;
    private Channel channel;

    // 存储requestId和Future的映射
    private final ConcurrentHashMap<Integer, CompletableFuture<RemotingCommand>> responseFutures
            = new ConcurrentHashMap<>();

    public NettyRemotingClient() {
        this.bootstrap = new Bootstrap();
        this.worderGroup = new NioEventLoopGroup();
    }

    public void start() {
        this.bootstrap.group(worderGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {

                    /**
                     * This method will be called once the {@link Channel} was registered. After the method
                     * returns this instance
                     * will be removed from the {@link ChannelPipeline} of the {@link Channel}.
                     *
                     * @param ch the {@link Channel} which was registered.
                     * @throws Exception is thrown if an error occurs. In that case it will be
                     *         handled by
                     *         {@link #exceptionCaught(ChannelHandlerContext, Throwable)} which will by
                     *         default close
                     *         the {@link Channel}.
                     */
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                // 5秒没发送数据，会触发一个IdleStateEvent
                                .addLast(new IdleStateHandler(0, 5, 0))
                                .addLast(new LengthFieldBasedFrameDecoder(1024*1024, 0, 4, 0, 4))
                                .addLast(new CommandEncoder())
                                .addLast(new CommandDecoder())
                                .addLast(new ClientHandler());
                    }
                });
    }

    public void connect(SocketAddress address) throws InterruptedException {
        ChannelFuture future = bootstrap.connect(address).sync();
        this.channel = future.channel();
        log.info("Client connected to {}", address);
    }

    public RemotingCommand invokeSync(RemotingCommand request, long timeoutMillis)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        responseFutures.put(request.getRequestId(), future);

        channel.writeAndFlush(request).addListener(channelFuture -> {
            if (!channelFuture.isSuccess()) {
                future.completeExceptionally(channelFuture.cause());
                responseFutures.remove(request.getRequestId());
            }
        });

        // 阻塞等待数据返回
        return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        worderGroup.shutdownGracefully();
    }

    class ClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        /**
         * Is called for each message of type {@link I}.
         *
         * @param ctx the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
         *         belongs to
         * @param msg the message to handle
         * @throws Exception is thrown if an error occurred
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand response) throws Exception {
            CompletableFuture<RemotingCommand> future = responseFutures.remove(response.getRequestId());
            if (future != null) {
                future.complete(response);
            } else {
                log.error("Received unexpected response: {}", response);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }

    }





}
