package com.echelon.hermes.common.utils;

/**
 * 描述：存储相关的工具类
 *
 * @author jorelwang
 * @create 2025-09-24 17:12
 */
public class StoreUtil {

    /**
     * CommitLog和ConsumeQueue文件名的长度
     */
    public static final int COMMIT_LOG_FILE_NAME_LENGTH = 20;

    /**
     * 私有构造函数，防止工具类被实例化。
     */
    private  StoreUtil() {
    }


    /**
     * 将一个长整型的偏移量格式化为定长的、左边补0的字符串。
     * @param offset    偏移量
     * @return  格式化后的20位字符串文件名
     */
    public static String offsetToFileName(final long offset) {
        final String format = "%0" + COMMIT_LOG_FILE_NAME_LENGTH + "d";
        return String.format(format, offset);
    }

}
