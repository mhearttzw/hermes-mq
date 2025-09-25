package com.echelon.hermes.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：消息存储的最终对外接口
 * CommitLog是消息存储的核心，所有消息都写入到CommitLog中
 *
 * @author jorelwang
 * @create 2025-09-24 17:28
 */
public class CommitLog {
    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    // 存储路径，例如./store/commitlog/
    private final String storePath;
    // 单个文件大小
    private final int mappedFileSize;
    // 文件队列管理器
    private final MappedFileQueue mappedFileQueue;

    // 这是一个可重入锁，用于保护所有对CommitLog的写入操作。
    // 确保在任何时刻，只有一个线程可以执行消息的追加逻辑，
    // 这对于保证消息的顺序性和内部数据结构（如MappedFileQueue）的一致性至关重要。
    protected final ReentrantLock putMessageLock = new ReentrantLock();


    public  CommitLog(String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.mappedFileQueue = new MappedFileQueue(storePath, mappedFileSize);
    }

    /**
     * 加载commitLog
     */
    public boolean load() {
        return this.mappedFileQueue.load();
    }

    public void shutdown() {
        this.mappedFileQueue.destroy();
    }

    /**
     * 对外提供的唯一写入接口
     * @param msg       内部消息对象
     * @return          写入结果
     */
    public PutMessageResult putMessage(final MessageExt msg) {
        // 设置存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());

        try {
            // 序列化消息
            byte[] messageData = serializeMessage(msg);
            final int messageLength = messageData.length;

            // todo 加锁
            this.putMessageLock.lock();

            // 获取最后一个能容纳本条消息的文件
            MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile(messageLength);
            if (lastMappedFile == null) {
                log.error("Failed to load last mapped file");
                return new PutMessageResult(PutMessageStatus.CREATE_MAP_FILE_FAILED, 0);
            }

            // 获取写入前的起始位置，就是该消息的物理偏移量
            long startOffset = lastMappedFile.getFileFromOffset() + lastMappedFile.getWrotePosition();

            // 追加消息到文件
            boolean success = lastMappedFile.appendMessage(messageData);

            if (success) {
                return new PutMessageResult(PutMessageStatus.PUT_OK, startOffset);
            } else {
                return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, 0);
            }

        } catch (IOException e) {
            // 如果重试后仍然失败，说明消息太大了
            log.error("Failed to serialize message", e);
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, -1);
        } finally {
            // 释放锁
            this.putMessageLock.unlock();
        }
    }


    /**
     * 根据物理偏移量读取消息
     * @param offset    物理偏移量
     * @return          消息对象
     */
    public MessageExt getMessage(final long offset) {
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            int pos = (int) (offset% this.mappedFileSize);
            try {
                // 先读取消息总长度，规定是4个字节
                ByteBuffer sizeBuffer = mappedFile.selectMappedBuffer(pos, 4);
                int messageSize = sizeBuffer.getInt();

                ByteBuffer dataBuffer = mappedFile.selectMappedBuffer(pos, messageSize);
                byte[] data = new byte[messageSize];
                dataBuffer.get(data);

                // 反序列化
                return deserializeMessage(data);
            } catch (Exception ex) {
                log.error("Failed to getMessage", ex);
            }
        }
        return null;
    }

    /**
     * 简易的java原生序列化方案
     * 格式：总长度（4字节） + MessageExt序列化内容
     * @param msg   消息
     */
    private byte[] serializeMessage(final MessageExt msg) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(msg);
        oos.flush();
        byte[] data = bos.toByteArray();

        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(4 + data.length);
        buffer.put(data);
        return buffer.array();
    }

    /**
     * java自身的反序列化方案
     * @param fullData      字节数组
     */
    private MessageExt deserializeMessage(final byte[] fullData) throws IOException, ClassNotFoundException {
        ByteBuffer buffer = ByteBuffer.wrap(fullData);
        // 跳过总长度字段
        buffer.getInt();

        int dataLength = fullData.length - 4;
        byte[] data = new byte[dataLength];
        buffer.get(data);

        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (MessageExt) ois.readObject();
    }


    class PutMessageResult {
        private PutMessageStatus status;
        // 消息的起始偏移量
        private long offset;
        public PutMessageResult(PutMessageStatus status, long offset) {
            this.status = status;
            this.offset = offset;
        }

        public PutMessageStatus getStatus() {
            return status;
        }

        public long getOffset() {
            return offset;
        }

    }

    enum PutMessageStatus {
        PUT_OK,
        CREATE_MAP_FILE_FAILED,
        MESSAGE_ILLEGAL,
    }
}
