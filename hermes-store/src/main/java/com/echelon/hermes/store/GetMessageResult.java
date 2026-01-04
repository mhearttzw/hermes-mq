package com.echelon.hermes.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GetMessageResult {

    // 存储所有消息的ByteBuffer列表
    private final List<ByteBuffer> messageBuffers = new ArrayList<>();
    private GetMessageStatus status;
    private long nextBeginOffset;
    private int bufferTotalSize;

    public int getMessageCount() {
        return this.messageBuffers.size();
    }

    public void addMessage(final ByteBuffer byteBuffer) {
        this.messageBuffers.add(byteBuffer);
        this.bufferTotalSize += byteBuffer.limit();
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public List<ByteBuffer> getMessageBuffers() {
        return messageBuffers;
    }


    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }
}
