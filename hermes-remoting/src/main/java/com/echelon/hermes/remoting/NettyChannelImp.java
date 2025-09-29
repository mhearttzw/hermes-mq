package com.echelon.hermes.remoting;

import com.echelon.hermes.protocol.RemotingCommand;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * 描述：netty的包装实现
 *
 * @author jorelwang
 * @create 2025-09-26 11:04
 */
public class NettyChannelImp implements RemotingChannel{

    // netty的channel
    private final Channel channel;

    public NettyChannelImp(Channel channel) {
        this.channel = channel;
    }

    /**
     * 向该通道写入并发送一个命令。
     *
     * @param cmd 要发送的命令
     * @return a ChannelFuture, 用于监听发送结果
     */
    @Override
    public ChannelFuture writeAndFlush(RemotingCommand cmd) {
        return this.channel.writeAndFlush(cmd);
    }

    /**
     * 获取远程地址
     *
     * @return a String representation of the remote address.
     */
    @Override
    public String getRemoteAddress() {
        return this.channel.remoteAddress().toString();
    }

    /**
     * 通道是否可用
     *
     * @return true if the channel is active.
     */
    @Override
    public boolean isActive() {
        return this.channel.isActive();
    }
}
