package com.echelon.hermes.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 描述：面向用户的消息对象。
 * 客户端API中用于发送和接收消息的核心数据结构。
 *
 * @author jorelwang
 * @create 2025-09-26 10:31
 */
public class Message implements Serializable {

    private static final long serialVersionUID = -1L;

    private String topic;
    private byte[] body;
    private Map<String, String> properties;

    public Message(String topic, byte[] body) {
        this.topic = topic;
        this.body = body;
        this.properties = new HashMap<>();
    }

    public void putProperty(String key, String value) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(key, value);
    }

    // --- Getters and Setters ---
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

}
