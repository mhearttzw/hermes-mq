package com.echelon.hermes.client;

import com.echelon.hermes.protocol.RemotingCommand;
import com.echelon.hermes.remoting.NettyRemotingClient;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：简易网络通信测试类（测试netty网络连接）
 * @author jorelwang
 * @create 2025/9/29 - 11:26
 */
public class PingPongClientTest {
    private static final Logger log = LoggerFactory.getLogger(PingPongClientTest.class);

    public static void main(String[] args) {
        NettyRemotingClient client = new NettyRemotingClient();
        client.start();

        try {
            // 连接到服务器
            client.connect(new InetSocketAddress("127.0.0.1", 8888));

            // 创建一个ping请求
            RemotingCommand request = RemotingCommand.createPingRequest();
            log.info("Client sending Ping request: {}", request);

            // 同步调用，等待响应
            RemotingCommand response = client.invokeSync(request, 60 * 1000);
            log.info("Client received Ping response: {}", response);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            client.shutdown();
        }


    }


}
