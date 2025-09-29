package com.echelon.hermes.broker;

import com.echelon.hermes.broker.processor.SendMessageProcessor;
import com.echelon.hermes.protocol.RemotingCommandCode;
import com.echelon.hermes.remoting.NettyRemotingServer;
import com.echelon.hermes.remoting.RequestProcessor;
import com.echelon.hermes.store.CommitLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：服务器启动类，组装网络服务和业务处理
 *
 * @author jorelwang
 * @create 2025-09-19 21:47
 */
public class BrokerLauncher {
    private static final Logger log = LoggerFactory.getLogger(BrokerLauncher.class);

    public static void main(String[] args) {
        // 1、创建并加载存储核心
        final CommitLog commitLog = new CommitLog("./store/commitlog", 1024*1024*100);
        if (!commitLog.load()) {
            log.info("Failed to load commitlog, broker not started");
            return;
        }

        // 2、创建网络服务器
        final NettyRemotingServer server = new NettyRemotingServer(8888);

        // 3、创建业务处理器
        final RequestProcessor sendMessageProcessor = new SendMessageProcessor(commitLog);

        // 4、【依赖注入】将处理器注册到网络服务器中
        server.registerProcessor(RemotingCommandCode.SEND_MESSAGE, sendMessageProcessor);

        // 5、创建并启动服务器
        server.start();

        // 6、添加一个关闭钩子，确保程序退出时能优雅地关闭服务器
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        log.info("Broker started");
    }

}
