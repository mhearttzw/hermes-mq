package com.echelon.hermes.broker.processor;

import com.echelon.hermes.protocol.RemotingCommandCode;
import com.echelon.hermes.protocol.SendMessageRequestHeader;
import com.echelon.hermes.protocol.SendMessageResponseHeader;
import com.echelon.hermes.remoting.RemotingChannel;
import com.echelon.hermes.remoting.RequestProcessor;
import com.echelon.hermes.protocol.RemotingCommand;
import com.echelon.hermes.store.CommitLog;
import com.echelon.hermes.store.MessageExt;
import com.echelon.hermes.store.PutMessageResult;
import com.echelon.hermes.store.PutMessageStatus;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：发送消息处理器，连接网络层和存储层的桥梁
 *
 * @author jorelwang
 * @create 2025-09-25 22:49
 */
public class SendMessageProcessor implements RequestProcessor {

    private final Logger log = LoggerFactory.getLogger(SendMessageProcessor.class);

    // commitLog实例
    private final CommitLog commitLog;

    public SendMessageProcessor(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    public RemotingCommand processRequest(RemotingChannel channel, RemotingCommand request) {
        try {
            // 1、只解码业务Header
            final SendMessageRequestHeader requestHeader =
                    request.decodeCustomHeader(SendMessageRequestHeader.class);

            // 2、构建内存存储对象MessageExt
            MessageExt msgExt = buildMessageExt(requestHeader, request, channel);

            // 3、调用commitLog写入消息
            PutMessageResult putResult = this.commitLog.putMessage(msgExt);

            // 4、根据写入结果，构建响应
            return buildResponse(putResult, request, msgExt,  channel);

        } catch (Exception e) {
            log.error("Process send message request failed from {}", channel.getRemoteAddress(), e);
            String remark = "Process request failed: " + e.getMessage();
            return RemotingCommand.createResponseCommand(RemotingCommandCode.SYSTEM_ERROR, remark,
                    request.getRequestId());
        }
    }

    private MessageExt buildMessageExt(SendMessageRequestHeader requestHeader, RemotingCommand request,
            RemotingChannel channel) {
        MessageExt msgExt = new MessageExt();
        msgExt.setTopic(requestHeader.getTopic());
        msgExt.setProperties(requestHeader.getProperties());

        // 消息体（body）被原封不动地传递
        msgExt.setBody(request.getBody());

        // 在broker端填充一些服务端才能确认的属性
        // todo uuid单独提取工具类
        msgExt.setBornTimestamp(requestHeader.getBornTimestamp());
        msgExt.setStoreTimestamp(System.currentTimeMillis());
        msgExt.setMsgId(UUID.randomUUID().toString());

        // 记录地址
        msgExt.putProperty("CLIENT_IP", channel.getRemoteAddress());
        return msgExt;
    }


    private RemotingCommand buildResponse(PutMessageResult putResult, RemotingCommand request,
            MessageExt msgExt, RemotingChannel channel) {
        if (putResult.getStatus() == PutMessageStatus.PUT_OK) {
            log.info("Message stored successfully, topic={}, msgId={}, from={}, offset={}",
                    msgExt.getTopic(), msgExt.getMsgId(), channel.getRemoteAddress(), putResult.getOffset());

            // 1、创建成功的响应对象
            String remark = "Put message OK, offset: " + putResult.getOffset();
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingCommandCode.SUCCESS, remark, request.getRequestId());

            // 2、填充响应header
            final SendMessageResponseHeader responseHeader = new SendMessageResponseHeader();
            responseHeader.setMsgId(msgExt.getMsgId());
            responseHeader.setOffset(putResult.getOffset());

            response.setCustomHeader(responseHeader);
            return response;
        } else {
            log.error("Failed to store message, topic={}, msgId={}, status={}, from={}",
                    msgExt.getTopic(), msgExt.getMsgId(), putResult.getStatus(), channel.getRemoteAddress());

            String remark = "Failed to store message, topic: " + msgExt.getTopic();
            return RemotingCommand.createResponseCommand(RemotingCommandCode.SYSTEM_ERROR, remark,
                    request.getRequestId());
        }
    }


    private MessageExt deserializeMessage(byte[] body) throws Exception {
        ByteArrayInputStream bis = new ByteArrayInputStream(body);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (MessageExt) ois.readObject();
    }


}
