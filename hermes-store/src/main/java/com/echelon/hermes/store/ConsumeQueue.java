package com.echelon.hermes.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费队列，存储消息在CommitLog中的索引
 * 格式：[CommitLogOffset(8)][Size(4)][TagCode(8)] = 20字节
 * @author jorelwang
 */
public class ConsumeQueue {
    private static final Logger log = LoggerFactory.getLogger(ConsumeQueue.class);
    
    public static final int CQ_STORE_UNIT_SIZE = 20;
    
    private final String storePath;
    private final String topic;
    private final int queueId;
    private final String fileName;
    
    private FileChannel fileChannel;
    private RandomAccessFile randomAccessFile;
    
    // 当前最大物理偏移量（CommitLog中的offset）
    private long maxPhysicOffset = -1;
    
    public ConsumeQueue(String storePath, String topic, int queueId) {
        this.storePath = storePath;
        this.topic = topic;
        this.queueId = queueId;
        this.fileName = storePath + File.separator + topic + File.separator + queueId + File.separator + "00000000000000000000";
        
        try {
            File file = new File(fileName);
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.randomAccessFile.getChannel();
        } catch (IOException e) {
            log.error("Failed to create ConsumeQueue", e);
        }
    }
    
    public boolean putMessagePositionInfo(long offset, int size, long tagsCode) {
        // 简单的重复校验
        if (offset <= this.maxPhysicOffset) {
            return true;
        }
        
        this.maxPhysicOffset = offset;
        
        ByteBuffer buffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        buffer.putLong(offset);
        buffer.putInt(size);
        buffer.putLong(tagsCode);
        buffer.flip();
        
        try {
            this.fileChannel.write(buffer, this.fileChannel.size());
            return true;
        } catch (IOException e) {
            log.error("Failed to write ConsumeQueue", e);
            return false;
        }
    }
    
    public ByteBuffer getIndexBuffer(long startIndex) {
        try {
            long position = startIndex * CQ_STORE_UNIT_SIZE;
            if (position >= this.fileChannel.size()) {
                return null;
            }
            
            // 读取所有剩余索引
            long size = this.fileChannel.size() - position;
            ByteBuffer buffer = ByteBuffer.allocate((int) size);
            this.fileChannel.read(buffer, position);
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            log.error("Failed to read ConsumeQueue", e);
            return null;
        }
    }
    
    public void shutdown() {
        try {
            if (this.fileChannel != null) {
                this.fileChannel.close();
            }
            if (this.randomAccessFile != null) {
                this.randomAccessFile.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public String getTopic() {
        return topic;
    }
    
    public int getQueueId() {
        return queueId;
    }
}
