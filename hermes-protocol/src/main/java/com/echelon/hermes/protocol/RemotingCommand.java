package com.echelon.hermes.protocol;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;

/**
 * 描述：网络传输的核心数据结构，请求、响应都是一个RemotingCommand对象。
 *
 * @author jorelwang
 * @create 2025-09-16 17:38
 */
@Data
public class RemotingCommand {

    // 序列化和ID唯一生产工具
    private static final AtomicInteger requestIdGenerator = new AtomicInteger(0);

    // -- header --
    private short code; // 请求、响应码
    private int requestId; // 请求ID，用于异步回调匹配
    private byte languageCode = LanguageCode.JAVA.getCode();
    private short version = 1; // 协议版本
    private String remark; // 备注信息

    // -- body --
    private transient byte[] body;

    // 元数据与数据分离
    // 存储业务层的自定义Header
    @JsonIgnore
    private transient CommandCustomHeader customHeader;

    /**
     * 存储customHeader序列化后的JSON字符串
     */
    private String extFields;

    /**
     * 在发送命令前，调用此方法，将customHeader对象编码成JSON字符串
     */
    public void encodeCustomHeader() throws JsonProcessingException {
        if (this.customHeader != null) {
            this.extFields = JsonUtils.toJson(this.customHeader);
        }
    }

    public <T extends CommandCustomHeader> T decodeCustomHeader(Class<T> headerClass) throws JsonProcessingException {
        if (this.extFields != null) {
            this.customHeader = JsonUtils.fromJson(this.extFields, headerClass);
            return (T) this.customHeader;
        }
        return null;
    }


    /**
     * 将header部分编码为JSON字符串，然后转化为byte[]
     */
    public byte[] encodeHeader() throws JsonProcessingException {
        return RemotingSerializable.encode(this);
    }

    /**
     * 将header部分的byte[]解码为RemotingCommand对象
     */
    public static RemotingCommand decodeHeader(byte[] headerData) {
        return RemotingSerializable.decode(headerData, RemotingCommand.class);
    }

    public static RemotingCommand createRequestCommand(short code) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRequestId(requestIdGenerator.incrementAndGet());
        return cmd;
    }

    public static RemotingCommand createResponseCommand(short code, String remark, int requestId) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.setRequestId(requestId);
        cmd.setRemark(remark);
        return cmd;
    }

    /**
     * 创建ping请求
     */
    public static RemotingCommand createPingRequest() {
        return createRequestCommand(RemotingCommandCode.PING);
    }

    /**
     * 创建pong响应
     */
    public static RemotingCommand createPongResponse(int requestId) {
        return createResponseCommand(RemotingCommandCode.SUCCESS, "PONG", requestId);
    }

    // Getters and Setters
    public CommandCustomHeader getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getExtFields() {
        return extFields;
    }

    public void setExtFields(String extFields) {
        this.extFields = extFields;
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code +
                ", requestId=" + requestId +
                ", languageCode=" + languageCode +
                ", version=" + version
                + ", remark=" + remark + "]";
    }


}
