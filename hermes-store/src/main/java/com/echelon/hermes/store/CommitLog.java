package com.echelon.hermes.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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

    // 持有新消息监听器的引用
    private volatile MessageArrivingListener messageArrivingListener;

    public void setMessageArrivingListener(MessageArrivingListener messageArrivingListener) {
        this.messageArrivingListener = messageArrivingListener;
    }

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

    public String getStorePath() {
        return storePath;
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
                // 发送消息到达事件，出发监听器
                if (this.messageArrivingListener != null) {
                    this.messageArrivingListener.onMessageArrived();
                }
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

    public GetMessageResult getMessage(final long offset, final int maxMsgNums) {
        GetMessageResult result = new GetMessageResult();
        // 获取当前CommitLog的最大物理偏移量，即写入指针的位置
        final long maxOffset = this.mappedFileQueue.getMaxOffset();

        // 1、严格的参数和边界校验
        if (offset < 0 || maxOffset <= 0) {
           result.setStatus(GetMessageStatus.OFFSET_ILLEGAL);
           result.setNextBeginOffset(offset);
           return result;
        }

        if (offset >= maxOffset) {
            result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            result.setNextBeginOffset(maxOffset);
            return result;
        }

        // 2、查找文件
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile == null) {
            // 找不到对应的MappedFile，通常是offset对应的文件被删了
            result.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
            // 返回第一个文件的起始offset，建议消费者从头开始
            result.setNextBeginOffset(this.mappedFileQueue.getFirstMappedFile().getFileFromOffset());
            return result;
        }

        // 3、从文件中循环读取消息
        int pos = (int) (offset% this.mappedFileSize);
        // 获取从当前位置到最新写入位置的数据
        ByteBuffer byteBufferView = mappedFile.selectMappedBuffer(pos);

        if (byteBufferView != null) {
            for (int i = 0; i < maxMsgNums && byteBufferView.hasRemaining(); i++) {
                // a、读取消息总长度（4字节）
                // 确保至少有4个字节可读，防止读取半条长度字段
                if (byteBufferView.remaining() < 4) {
                    break;
                }
                final int messageSize = byteBufferView.getInt();

                // b、校验消息长度的合法性
                if (messageSize <= 0 || messageSize > byteBufferView.remaining()) {
                    // 读到了文件末尾的空白区域或损坏数据
                    break;
                }

                // c、读取消息，并添加到结果列表中
                ByteBuffer messageBuffer = byteBufferView.slice();
                messageBuffer.limit(messageSize - 4); // limit是消息内容，不包含长度自身

                result.addMessage(messageBuffer);

                // d、更新byteBufferView的position，跳过这条消息的剩余部分
                // (messageSize - 4) 是因为我们已经用getInt()消费了4个字节
                byteBufferView.position(byteBufferView.position() + messageSize - 4);
            }
        }

        // 4、设置最终状态和下一个偏移量
        if (result.getMessageCount() == 0) {
            result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            // 如果没有任何消息，则返回当前offset
            result.setNextBeginOffset(offset);
        } else {
            result.setStatus(GetMessageStatus.FOUND);
            // 如果有消息，则返回下一条消息的起始offset
            result.setNextBeginOffset(offset + result.getBufferTotalSize());
        }

        return result;

    }


    /**
     * 根据物理偏移量读取消息，目前用于测试存储消息功能是否正确
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



}
