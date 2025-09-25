package com.echelon.hermes.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.echelon.hermes.store.CommitLog.PutMessageResult;
import com.echelon.hermes.store.CommitLog.PutMessageStatus;
import java.io.File;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：消息存储层测试类
 *
 * @author jorelwang
 * @create 2025-09-25 10:01
 */
public class CommitLogTest {
    private static final Logger log = LoggerFactory.getLogger(CommitLogTest.class);

    private CommitLog commitLog;
    private final String storePath = "./unittteststore/commitlog";

    @BeforeEach
    public void SetUp() {
        // 每次测试前，清空并创建测试目录
        File dir = new File(storePath);
        deleteDir(dir);
        dir.mkdirs();

        commitLog = new CommitLog(storePath, 1024 *1024 *10);
        assertTrue(commitLog.load());
    }

    // @AfterEach
    // public void tearDown() {
    //     commitLog.shutdown();
    //     // 测试后删除目录
    //     deleteDir(new File("./unitteststore"));
    // }

    @Test
    public void testPutAndGetMessage() {
        // 1、准备一条消息
        MessageExt msg = new MessageExt();
        msg.setTopic("TestTopic");
        msg.setBody("Hello, Hermes MQ!".getBytes());
        msg.putProperty("user", "test");

        // 2、写入消息
        PutMessageResult result = commitLog.putMessage(msg);
        assertEquals(PutMessageStatus.PUT_OK, result.getStatus());
        assertTrue(result.getOffset() >= 0);
        assertEquals(0, result.getOffset()); // 第一条的消息偏移量是0
        log.info("Message put at offset: {}", result.getOffset());

        // 3、读取消息
        MessageExt readMsg = commitLog.getMessage(result.getOffset());

        // 4、断言验证
        assertNotNull(readMsg);
        assertEquals("TestTopic",  readMsg.getTopic());
        assertArrayEquals("Hello, Hermes MQ!".getBytes(), readMsg.getBody());
        assertEquals("test", readMsg.getProperties().get("user"));

        // 5. 写入第二条消息，以验证偏移量是递增的
        MessageExt msg2 = new MessageExt();
        msg2.setTopic("TestTopic");
        msg2.setBody("Second message.".getBytes());
        msg2.putProperty("user", "test2");

        PutMessageResult result2 = commitLog.putMessage(msg2);
        assertEquals(PutMessageStatus.PUT_OK, result2.getStatus());

        // 【增强】断言第二条消息的偏移量大于第一条
        assertTrue(result2.getOffset() > result.getOffset());
        log.info("Second message put at offset: {}", result2.getOffset());

        // 6. 读取并验证第二条消息
        MessageExt readMsg2 = commitLog.getMessage(result2.getOffset());
        assertNotNull(readMsg2);
        assertArrayEquals("Second message.".getBytes(), readMsg2.getBody());

    }

    private void deleteDir(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteDir(child);
                }
            }
        }
        file.delete();
    }

}
