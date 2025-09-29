package com.echelon.hermes.client;

import com.echelon.hermes.client.SendResult.SendStatus;
import com.echelon.hermes.common.Message;
import com.echelon.hermes.protocol.JsonUtils;
import com.echelon.hermes.remoting.NettyRemotingClient;
import com.echelon.hermes.protocol.RemotingCommand;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：客户端测试类，端到端测试
 *
 * 测试以下关键链路：
 * 1、协议的正确封装（SendMessageRequestHeader、SendMessageResponseHeader）
 * 2、客户端与服务器的同步网络通信
 * 3、broker端处理器（SendMessageProcessor）的业务逻辑
 * 4、broker端消息的持久化（commitLog）
 *
 * @author jorelwang
 * @create 2025-09-19 21:52
 */
public class ClientTest {
    private static final Logger log = LoggerFactory.getLogger(ClientTest.class);

    public static void main(String[] args) {
        NettyRemotingClient client = new NettyRemotingClient();
        log.info("-- hermes client start --");

        // broker地址，确保与brokerLauncher中的端口一致
        final String brokerAddress = "127.0.0.1:8888";
        HermesProducerImpl producer = new HermesProducerImpl(brokerAddress);

        try {
            // 1、启动生产者
            producer.start();
            log.info("-- Producer started --");

            // 2、发送第一条消息
            log.info("Sending first message");
            byte[] body = "Hello, Hermes MQ! This is the first message.".getBytes(StandardCharsets.UTF_8);
            Message msg1 = new Message("TestTopic-1", body);
            msg1.putProperty("property-key1", "property-value1");

            SendResult result1 = producer.send(msg1);

            // 3、验证第一条消息的发送结果
            log.info("Received result for first message:{}", JsonUtils.toJson(result1));
            assertSendResultOK(result1, 0L);


            // 4、发送第二条消息
            log.info("Sending second message");
            byte[] body2 =
                    "Hello, Hermes MQ! This is the seconde message.".getBytes(StandardCharsets.UTF_8);
            Message msg2 = new Message("TestTopic-1", body2);
            msg2.putProperty("property-key2", "property-value2");

            SendResult result2 = producer.send(msg2);

            // 5、验证第二条消息的发送结果
            log.info("Received result for second message:{}", msg2);
            assertSendResultOK(result2, null);
            if (result2.getOffset() <= result1.getOffset()) {
                throw new AssertionError(String.format(
                        "Assertion failed: Second offset (%d) should be greater than first offset (%d).",
                        result2.getOffset(), result1.getOffset()
                ));
            }

            log.info("-- Hermes client Test Passed!");

        } catch (Exception e) {
            e.printStackTrace();
            log.error("--- Hermes Client E2E Test Failed! ---", e);
            throw new RuntimeException(e);
        } finally {
            // 6、关闭生产者
            log.info("Shutting down producer...");
            producer.shutdown();
            log.info("Producer shut down!");
        }

    }


    private static void assertSendResultOK(SendResult result, Long expectedOffset) {
        if (result == null) {
            throw new AssertionError("SendResult is null");
        }
        if (result.getSendStatus() != SendStatus.SEND_OK) {
            throw new AssertionError(String.format(
                    "Assertion failed: Expected SendStatus.SEND_OK, but got %s. Remark: %s",
                    result.getSendStatus(), result.getRemark()
            ));
        }
        if (result.getMsgId() == null || result.getMsgId().isEmpty()) {
            throw new AssertionError("Assertion failed: Message ID is null or empty. Remark: " + result.getRemark());
        }
        if (result.getOffset() < 0) {
            throw new AssertionError("Assertion failed: Offset is negative: " + result.getOffset());
        }
        if (expectedOffset != null && result.getOffset() != expectedOffset) {
            throw new AssertionError("Assertion failed: Offset expeceted but got " + result.getOffset());
        }

    }




}
