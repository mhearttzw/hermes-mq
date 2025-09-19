package com.echelon.hermes.broker;

import com.echelon.hermes.remoting.NettyRemotingClient;
import com.echelon.hermes.remoting.NettyRemotingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：服务器启动类
 *
 * @author jorelwang
 * @create 2025-09-19 21:47
 */
public class BrokerLauncher {
    private static final Logger log = LoggerFactory.getLogger(BrokerLauncher.class);

    public static void main(String[] args) {
        // 创建并启动服务器
        NettyRemotingServer server = new NettyRemotingServer(8888);
        server.start();

        // 添加一个关闭钩子，确保程序退出时能优雅地关闭服务器
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        log.info("Broker started");
    }

}
