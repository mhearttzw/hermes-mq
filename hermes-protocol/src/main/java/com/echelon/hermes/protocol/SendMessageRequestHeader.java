package com.echelon.hermes.protocol;

import java.util.Map;
import java.util.Objects;

/**
 * 描述：发送消息请求的自定义业务Header
 * 这个类包含了所有用于broker决策和路由的元数据，而不包含实际的消息体（body）
 * 实现了CommandCustomHeader接口，可以被序列化为JSON并放入RemotingCommand的extensibleFields字段中。
 *
 * @author jorelwang
 * @create 2025-09-28 10:31
 */
public class SendMessageRequestHeader implements CommandCustomHeader {

    /**
     * 消息的主题
     */
    private String topic;

    /**
     * 消息的队列ID
     * 生产者可以指定将消息发送到topic下的哪一个具体队列
     * 如果为null或-1，broker可以根据策略（如轮询）来选择一个队列
     */
    private Integer queueId;

    /**
     * 消息的自定义属性
     * 可以用于业务级别的标记、过滤的。
     */
    private Map<String, String> properties;

    /**
     * 消息在生产端的创建时间戳
     * 用于消息追踪和延迟计算等。
     */
    private Long bornTimestamp;

    // --- Getters and Setters ---

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    // --- 重写 equals, hashCode, toString 方法，便于测试和调试 ---

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SendMessageRequestHeader that = (SendMessageRequestHeader) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(queueId, that.queueId) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(bornTimestamp, that.bornTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, queueId, properties, bornTimestamp);
    }

    @Override
    public String toString() {
        return "SendMessageRequestHeader{" +
                "topic='" + topic + '\'' +
                ", queueId=" + queueId +
                ", properties=" + properties +
                ", bornTimestamp=" + bornTimestamp +
                '}';
    }

}
