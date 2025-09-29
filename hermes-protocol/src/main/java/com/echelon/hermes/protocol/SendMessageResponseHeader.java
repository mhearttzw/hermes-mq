package com.echelon.hermes.protocol;

import java.util.Objects;

/**
 * 描述：发送消息响应的自定义业务Header
 * 携带消息在broker端被成功处理后的关键信息
 *
 * @author jorelwang
 * @create 2025-09-28 17:20
 */
public class SendMessageResponseHeader implements CommandCustomHeader {

    /**
     * 消息在broker端生产的全局唯一ID
     */
    private String msgId;

    /**
     * 消息被写入CommitLog的物理偏移量
     */
    private Long offset;

    // --- Getters and Setters ---

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    // --- equals, hashCode, toString ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SendMessageResponseHeader that = (SendMessageResponseHeader) o;
        return Objects.equals(msgId, that.msgId) &&
                Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msgId, offset);
    }

    @Override
    public String toString() {
        return "SendMessageResponseHeader{" +
                "msgId='" + msgId + '\'' +
                ", offset=" + offset +
                '}';
    }
}
