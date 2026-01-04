package com.echelon.hermes.store;

import com.echelon.hermes.common.Message;

/**
 * 描述：新消息到达的监听器接口
 * 当CommitLog有新消息写入时，会回调此接口
 *
 * @author jorelwang
 * @create 2025-10-11 17:34
 */
public interface MessageArrivingListener {

    /**
     * 当新消息到达时被调用
     * @param message   消息
     */
    // void onMessageArriving(Message message);
    void onMessageArrived();

}
