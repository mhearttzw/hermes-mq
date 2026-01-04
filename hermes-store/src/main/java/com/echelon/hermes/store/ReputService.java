package com.echelon.hermes.store;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 重新分发服务
 * 负责从CommitLog读取消息，并分发到ConsumeQueue
 * @author jorelwang
 */
public class ReputService extends Thread {
    private static final Logger log = LoggerFactory.getLogger(ReputService.class);
    
    private final CommitLog commitLog;
    private volatile boolean stopped = false;
    private long reputFromOffset = 0;
    
    // topic-queueId -> ConsumeQueue
    private final Map<String, ConsumeQueue> consumeQueueTable = new ConcurrentHashMap<>();
    
    public ReputService(CommitLog commitLog) {
        this.commitLog = commitLog;
    }
    
    @Override
    public void run() {
        log.info("ReputService started.");
        
        while (!stopped) {
            try {
                doReput();
                Thread.sleep(1);
            } catch (Exception e) {
                log.error("ReputService error", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {}
            }
        }
        log.info("ReputService stopped.");
    }
    
    private void doReput() {
        // 从CommitLog读取一批数据
        GetMessageResult result = commitLog.getMessage(reputFromOffset, 32);
        
        if (result == null) {
            return;
        }
        
        if (result.getStatus() == GetMessageStatus.FOUND) {
            List<ByteBuffer> messageList = result.getMessageBuffers();
            
            for (ByteBuffer buffer : messageList) {
                try {
                    // 反序列化消息
                    // 注意：这里的buffer是消息体，不包含前4个字节的总长度
                    int bodySize = buffer.limit();
                    int totalSize = bodySize + 4;
                    
                    byte[] data = new byte[bodySize];
                    buffer.get(data); // 读取数据会改变position，所以需要slice或者get后复位？
                    // GetMessageResult里的buffer是独立的slice，可以放心读
                    
                    MessageExt msg = deserializeMessage(data);
                    msg.setStoreSize(totalSize);
                    
                    // 分发到ConsumeQueue
                    ConsumeQueue cq = findConsumeQueue(msg.getTopic(), msg.getQueueId());
                    
                    // 这里的reputFromOffset就是这条消息的物理offset
                    // 但我们需要确保循环中 offset 累加正确
                    // 实际上 GetMessageResult 是一批连续的消息
                    // 当前消息的 offset = reputFromOffset
                    // 下一条 = reputFromOffset + totalSize
                    
                    msg.setCommitLogOffset(reputFromOffset);
                    
                    cq.putMessagePositionInfo(reputFromOffset, totalSize, msg.getTagsCode());
                    
                    // 推进
                    reputFromOffset += totalSize;
                    
                } catch (Exception e) {
                    log.error("Reput message failed", e);
                }
            }
        } else if (result.getStatus() == GetMessageStatus.NO_MESSAGE_IN_QUEUE) {
            // 没有新消息，确保 offset 对齐
            reputFromOffset = result.getNextBeginOffset();
        } else if (result.getStatus() == GetMessageStatus.OFFSET_TOO_SMALL) {
            // 偏移量太小（文件被删），重置
            reputFromOffset = result.getNextBeginOffset();
        }
    }
    
    private MessageExt deserializeMessage(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (MessageExt) ois.readObject();
    }
    
    private ConsumeQueue findConsumeQueue(String topic, int queueId) {
        String key = topic + "-" + queueId;
        return consumeQueueTable.computeIfAbsent(key, k -> 
            new ConsumeQueue(commitLog.getStorePath(), topic, queueId) // 这里 storePath 可能是文件的父目录，假设 ConsumeQueue 会处理
        );
    }
    
    public void shutdown() {
        this.stopped = true;
        for (ConsumeQueue cq : consumeQueueTable.values()) {
            cq.shutdown();
        }
    }
}
