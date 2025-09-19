package com.echelon.hermes.remoting.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：序列化工具类
 *
 * @author jorelwang
 * @create 2025-09-19 22:26
 */
public class RemotingSerializable {
    private static final Logger log = LoggerFactory.getLogger(RemotingSerializable.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    public static byte[] encode(Object obj) {
        try {
            byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(obj);
            log.info("原始数据转字符串：{}", new String(jsonBytes, StandardCharsets.UTF_8));
            return jsonBytes;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Encode header failed", e);
        }


    }

    public static <T> T decode(byte[] data, Class<T> clazzOfT) {
        try {
            String strData = new String(data, StandardCharsets.UTF_8);
            log.info("strData:{}", strData);
            return OBJECT_MAPPER.readValue(data, clazzOfT);
        } catch (IOException e) {
            throw new RuntimeException("Decode header failed", e);
        }

    }

}
