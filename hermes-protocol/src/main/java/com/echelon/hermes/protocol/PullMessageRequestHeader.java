package com.echelon.hermes.protocol;

/**
 * 描述：拉取消息请求-自定义业务消息头
 *
 * @author jorelwang
 * @create 2025-10-11 17:21
 */
public class PullMessageRequestHeader implements CommandCustomHeader {

    /**
     * 消费者组
     */
    private String consumerGroup;

    /**
     * 消息主题
     */
    private String topic;

    /**
     * 队列ID
     */
    private Integer queueId;

    /**
     * 拉取消息位置
     */
    private Long queueOffset;

    /**
     * 本次最多拉取多少条消息
     */
    private Integer maxMsgNums;

    /**
     * 长轮询模式下，broker挂起请求的最长时间（毫秒）
     */
    private Long suspendTimeoutMillis;


    // --- Getters and Setters ---
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

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

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public Integer getMaxMsgNums() {
        return maxMsgNums;
    }

    public void setMaxMsgNums(Integer maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }

    public Long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public void setSuspendTimeoutMillis(Long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
    }


}
