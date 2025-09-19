package com.echelon.hermes.remoting;

import com.echelon.hermes.common.RemotingCommandCode;
import com.echelon.hermes.remoting.protocol.CommandDecoder;
import com.echelon.hermes.remoting.protocol.CommandEncoder;
import com.echelon.hermes.remoting.protocol.RemotingCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：Netty的服务端和客户端
 *
 * @author jorelwang
 * @create 2025-09-18 21:47
 */
public class NettyRemotingServer {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingServer.class);


    // netty服务端启动对象
    private final ServerBootstrap serverBootstrap;
    // netty boss 组线程池 一般只有一个线程。
    private final EventLoopGroup bossGroup;
    // netty worker 组线程池
    private final EventLoopGroup workerGroup;
    // 服务器绑定的端口，启动之后会改为config设置的端口
    private int port = 0;

    public NettyRemotingServer(int port) {
        this.port = port;
        this.serverBootstrap = new ServerBootstrap();
        this.bossGroup = new NioEventLoopGroup(1); // 负责处理连接
        this.workerGroup = new NioEventLoopGroup(); // 负责处理IO事件
    }

    public void start() {
        this.serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true) // 客户端ch选项
                .localAddress(new InetSocketAddress(port))  // 设置服务器端口
                .handler(new LoggingHandler(LogLevel.INFO)) // BossGroup的日志
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // 初始化客户端 ch pipeline的逻辑
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(
                                        1024 * 1024,
                                        0,      // lengthFieldOffset: 总长度字段的偏移量
                                        4,                    // lengthFieldLength: 总长度字段的长度
                                        0,                    // lengthAdjustment
                                        4                     // initialBytesToStrip: 剥离掉总长度字段
                                ))
                                .addLast(new CommandEncoder())
                                .addLast(new CommandDecoder())
                                .addLast(new ServerHandler()); // 自己的业务处理器
                    }
                });

        try {
            ChannelFuture future = serverBootstrap.bind().sync();
            log.info("NettyRemotingServer started and listen on port: {}", port);
        } catch (InterruptedException e) {
            throw new RuntimeException("Start server failed", e);
        }
    }

    public void shutdown() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Sharable
    class ServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        /**
         * Is called for each message of type {@link I}.
         *
         * @param ctx the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
         *         belongs to
         * @param msg the message to handle
         * @throws Exception is thrown if an error occurred
         */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
            log.info("Server received command: {}", request);
            if (Objects.equals(request.getCode(), RemotingCommandCode.PING)) {
                // 如果是PING请求，回复一个PONG响应
                RemotingCommand response = RemotingCommand.createPongResponse(request.getRequestId());
                ctx.writeAndFlush(response);
            } else {
                log.info("Server received unknown command: {}", request);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }

}
