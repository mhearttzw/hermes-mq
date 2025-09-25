package com.echelon.hermes.store;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 描述：消息的内部存储结构。
 * 'Ext' 表示'Extended'，意味着它比客户端发送的Message包含了更多Broker端的内部属性。
 * @author jorelwang
 * @create 2025-09-21 00:29
 */
public class MessageExt implements Serializable {

    private static final long serialVersionUID = -800418382728218903L;

    // 消息的全局唯一ID
    private String msgId;

    // 消息主题
    private String topic;

    // 消息体
    private byte[] body;

    // 用户自定义属性
    private Map<String, String> properties;

    // 消息在客户端的创建时间戳
    private long bornTimestamp;

    // 消息在broker端的存储时间戳
    private long storeTimestamp;

    // 消息在commitLog中的物理偏移量
    private long commitLogOffset;

    public MessageExt() {
        this.properties = new HashMap<>();
    }

    // --- Getters and Setters ---
    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void putProperty(String key, String value) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, value);
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    @Override
    public String toString() {
        return "MessageExt{" +
                "msgId='" + msgId + '\'' +
                ", topic='" + topic + '\'' +
                ", commitLogOffset=" + commitLogOffset +
                '}';
    }

}
