package com.echelon.hermes.common;

// in: hermes-common/src/main/java/.../common/RemotingCommandCode.java
public class RemotingCommandCode {
    // 请求类型
    public static final short SEND_MESSAGE = 10;
    public static final short PULL_MESSAGE = 11;

    // 响应类型
    public static final short SUCCESS = 200;
    public static final short SYSTEM_ERROR = 500;
    
    // 我们测试用的心跳或Ping/Pong
    public static final short PING = 99;
}