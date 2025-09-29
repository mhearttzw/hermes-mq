package com.echelon.hermes.remoting;

import com.echelon.hermes.protocol.RemotingCommand;

/**
 * 描述：TODO...
 *
 * @author jorelwang
 * @create 2025-09-26 11:02
 */
public interface RequestProcessor {

    /**
     * 处理请求。
     * @param channel 代表网络连接的通道
     * @param request 请求命令
     * @return a RemotingCommand作为响应，如果不需要立刻响应，可以返回null
     */
    RemotingCommand processRequest(RemotingChannel channel, RemotingCommand request);





}
