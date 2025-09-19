package com.echelon.hermes.client;

import com.echelon.hermes.remoting.NettyRemotingClient;
import com.echelon.hermes.remoting.protocol.RemotingCommand;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：客户端测试类
 *
 * @author jorelwang
 * @create 2025-09-19 21:52
 */
public class ClientTest {
    private static final Logger log = LoggerFactory.getLogger(ClientTest.class);

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
            RemotingCommand response = client.invokeSync(request, 3000);
            log.info("Client received Ping response: {}", response);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            client.shutdown();
        }


    }


}
