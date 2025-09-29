package com.echelon.hermes.client;

import com.echelon.hermes.common.Message;

/**
 * 描述：TODO...
 *
 * @author jorelwang
 * @create 2025-09-26 14:49
 */
public interface HermesProducer {

    void start();
    void shutdown();

    SendResult send(Message message) throws Exception;

}
