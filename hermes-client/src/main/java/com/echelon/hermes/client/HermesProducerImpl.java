package com.echelon.hermes.client;

import com.echelon.hermes.client.SendResult.SendStatus;
import com.echelon.hermes.common.Message;
import com.echelon.hermes.common.ServiceState;
import com.echelon.hermes.protocol.RemotingCommandCode;
import com.echelon.hermes.protocol.SendMessageRequestHeader;
import com.echelon.hermes.protocol.SendMessageResponseHeader;
import com.echelon.hermes.remoting.NettyRemotingClient;
import com.echelon.hermes.protocol.RemotingCommand;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：消息生产者实现类
 * 封装了与broker的网络通信细节，提供send()方法
 * <p>
 * 工作流程：
 * 1、在start()时，初始化并连接底层的netty客户端
 * 2、调用send方法
 * a、将用户传入的Message对象，转换为网络协议所需的SendMessageRequestHeader和纯消息体（body）
 * b、构建RemotingCommand网络请求对象
 * c、调用NettyRemotingClient的同步方法发送请求并等待响应
 * d、解析RemotingCommand响应，并将其转换为对用户友好的SendResult对象
 *
 * @author jorelwang
 * @create 2025-09-26 14:50
 */
public class HermesProducerImpl implements HermesProducer {

    private static final Logger log = LoggerFactory.getLogger(HermesProducerImpl.class);
    // 底层的Netty网络通信客户端
    private final NettyRemotingClient remotingClient;
    // 要连接的broker地址，未来这里可以升级为一个动态获取地址的服务发现组件
    private final String brokerAddress;

    public HermesProducerImpl(String brokerAddress) {
        this.brokerAddress = brokerAddress;
        this.remotingClient = new NettyRemotingClient();
    }

    // 生产者的运行状态
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    @Override
    public void start() {
        synchronized (this) {
            switch (serviceState) {
                case CREATE_JUST:
                    this.serviceState = ServiceState.START_FAILED;
                    log.info("Starting Hermes producer for broker address {}", brokerAddress);
                    try {
                        // 1、启动底层网络客户端
                        this.remotingClient.start();

                        // 2、连接到broker
                        String[] addrParts = this.brokerAddress.split(":");
                        String host = addrParts[0];
                        int port = Integer.parseInt(addrParts[1]);
                        this.remotingClient.connect(new InetSocketAddress(host, port));

                        // 3、所有启动步骤都成功后，将状态更新为RUNNING
                        this.serviceState = ServiceState.RUNNING;
                        log.info("Hermes producer started for broker address {}", brokerAddress);
                    } catch (Exception e) {
                        // 确保资源被关闭
                        this.remotingClient.shutdown();
                        log.error("HermesProducer failed to start.", e);
                        // 抛出异常，让调用者感知到启动失败
                        throw new RuntimeException("HermesProducer start failed.", e);
                    }
                    break;
                case RUNNING:
                    log.info("Hermes producer started.");
                    break;
                case START_FAILED:
                    throw new IllegalStateException(
                            "HermesProducer failed to start last time, please create a new instance.");
                case SHUTDOWN_ALREADY:
                    // 如果已经关闭，抛出异常，不允许再次启动
                    throw new IllegalStateException("HermesProducer has been shut down, cannot be started again.");
                default:
                    break;

            }
        }

    }

    @Override
    public void shutdown() {
        this.remotingClient.shutdown();
    }

    @Override
    public SendResult send(Message msg) throws Exception {
        // 判断state状态
        if (this.serviceState != ServiceState.RUNNING) {
            throw new RuntimeException("Service is not running");
        }

        // 1、创建业务Header
        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
        requestHeader.setTopic(msg.getTopic());
        requestHeader.setProperties(msg.getProperties());
        requestHeader.setBornTimestamp(System.currentTimeMillis());

        // 2、创建网络请求
        RemotingCommand request =
                RemotingCommand.createRequestCommand(RemotingCommandCode.SEND_MESSAGE);

        // 3、设置header头、body消息体
        request.setCustomHeader(requestHeader);
        request.setBody(msg.getBody());

        // 4、在发送消息前，将customHeader编码成字符串
        request.encodeCustomHeader();

        // 5、发送消息
        RemotingCommand response = this.remotingClient.invokeSync(request, 120*1000);

        // 6、解析响应并构建SendResult
        return parseResponse(response);
    }

    /**
     * 将网络层的RemotingCommand响应，解析成面向用户的SendResult
     */
    private SendResult parseResponse(RemotingCommand response) {
        SendResult sendResult = new SendResult();
        sendResult.setRemark(response.getRemark());

        // 根据响应码判断基本状态
        if (response.getCode() == RemotingCommandCode.SUCCESS) {
            sendResult.setSendStatus(SendStatus.SEND_OK);
        } else {
            sendResult.setSendStatus(SendStatus.SEND_FAILED);
        }

        try {
            // 解码自定义Header
            SendMessageResponseHeader responseHeader =
                    response.decodeCustomHeader(SendMessageResponseHeader.class);

            if (responseHeader != null) {
                sendResult.setMsgId(responseHeader.getMsgId());
                sendResult.setOffset(responseHeader.getOffset());
            } else {
                if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                    log.warn("Received a success response, "
                            + "but SendMessageResponseHeader is null. Response: {}", response);
                }
            }
        } catch (Exception e) {
            sendResult.setSendStatus(SendStatus.SEND_FAILED);
        }

        return sendResult;
    }



}
