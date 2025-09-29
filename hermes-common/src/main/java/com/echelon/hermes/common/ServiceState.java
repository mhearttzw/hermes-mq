package com.echelon.hermes.common;

/**
 * 描述：服务状态的枚举
 * 用于管理生产者的生命周期
 *
 * @author jorelwang
 * @create 2025-09-28 15:34
 */
public enum ServiceState {

    /**
     * 初始状态，刚刚创建
     */
    CREATE_JUST,

    /**
     * 正在运行
     */
    RUNNING,

    /**
     * 已经关闭
     */
    SHUTDOWN_ALREADY,

    /**
     * 启动失败
     */
    START_FAILED

}
