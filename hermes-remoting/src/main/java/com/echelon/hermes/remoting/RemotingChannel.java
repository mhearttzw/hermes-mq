package com.echelon.hermes.remoting;

import com.echelon.hermes.protocol.RemotingCommand;
import io.netty.channel.ChannelFuture;

/**
 * 描述：远程连接通道的抽象接口
 *  向其他模块提供的、与具体网络无关的接口
 * @author jorelwang
 * @create 2025-09-26 10:58
 */
public interface RemotingChannel {
    /**
     * 向该通道写入并发送一个命令。
     * @param cmd 要发送的命令
     * @return a ChannelFuture, 用于监听发送结果
     */
    ChannelFuture writeAndFlush(RemotingCommand cmd);

    /**
     * 获取远程地址
     * @return a String representation of the remote address.
     */
    String getRemoteAddress();

    /**
     * 通道是否可用
     * @return true if the channel is active.
     */
    boolean isActive();




}
