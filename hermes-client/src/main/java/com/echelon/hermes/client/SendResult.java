package com.echelon.hermes.client;

/**
 * 描述：发送消息结果类
 *
 * @author jorelwang
 * @create 2025-09-26 14:46
 */
public class SendResult {

    private SendStatus sendStatus;
    private String msgId;
    private long offset;
    private String remark;

    // getters and setters
    public String  getRemark() {
        return remark;
    }
    public void setRemark(String remark) {
        this.remark = remark;
    }

    public SendStatus getSendStatus() {
        return sendStatus;
    }
    public void setSendStatus(SendStatus sendStatus) {
        this.sendStatus = sendStatus;
    }

    public String getMsgId() {
        return msgId;
    }
    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
    public long getOffset() {
        return offset;
    }
    public void setOffset(long offset) {
        this.offset = offset;
    }

    public enum SendStatus {
        SEND_OK,
        SEND_FAILED,
    }
}
