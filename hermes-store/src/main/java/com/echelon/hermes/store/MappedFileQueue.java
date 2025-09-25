package com.echelon.hermes.store;

import com.echelon.hermes.common.utils.StoreUtil;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 描述：文件队列管理器
 *
 * @author jorelwang
 * @create 2025-09-24 12:12
 */
public class MappedFileQueue {

    private static final Logger log = LoggerFactory.getLogger(MappedFileQueue.class);

    // 存储目录路径
    private final String storePath;
    // 单个MappedFile的大小
    private final int mappedFileSize;
    // 存储所有MappedFile实例的线程安全列表
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    public MappedFileQueue(final String storePath, final int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    /**
     * 加载存储目录下的所有文件，在broker启动时调用
     */
    public boolean load() {
        File dir = new File(storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // 按文件名（起始偏移量）排序
            Arrays.sort(files);
            for (File file : files) {
                // 必须是数字文件名，且文件大小与配置一致
                if (file.length() != this.mappedFileSize) {
                    log.error("File {} size {} not matched with mappedFileSize {}",
                            file.getName(), file.length(), this.mappedFileSize);
                    return false;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), this.mappedFileSize);
                    // 初始化写入位置为文件末尾
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                } catch (Exception e) {
                    log.error("Error while loading file {}, {}", file.getPath(), e.getMessage());
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 获取目录下的最后一个MappedFile
     * 如果最后一个文件已满，或者列表为空，则会创建一个新的文件
     *
     * @return MappedFile实例
     */
    public MappedFile getLastMappedFile() {
        MappedFile lastMappedFile = null;

        if (!this.mappedFiles.isEmpty()) {
            lastMappedFile = this.mappedFiles.get(this.mappedFiles.size() - 1);
        }

        if (lastMappedFile == null || lastMappedFile.isFull()) {
            lastMappedFile = createNewMappedFile(lastMappedFile);
        }
        return lastMappedFile;

    }

    public MappedFile getLastMappedFile(final int needSize) {
        // 如果期望写入的长度大于单个文件大小，直接返回null，表示无法处理
        if (needSize > this.mappedFileSize) {
            log.warn("Message size {} is larger than mapped file size {}, can not be written.",
                    needSize, this.mappedFileSize);
            return null;
        }

        MappedFile lastMappedFile = null;
        if (!this.mappedFiles.isEmpty()) {
            lastMappedFile = this.mappedFiles.get(this.mappedFiles.size() - 1);
        }

        // 如果没有文件，或者最后一个文件剩余空间不足
        if (lastMappedFile == null ||
                lastMappedFile.getFileSize() - lastMappedFile.getWrotePosition() < needSize) {
            return createNewMappedFile(lastMappedFile);
        }

        return lastMappedFile;
    }

    /**
     * 创建新的mappedFile对象
     * rocketMq中是专门有一个线程负责创建新mappedFile实例
     *
     * @param oldMappedFile 当前目录下已存在的最新的mappedFile
     * @return mappedFile对象
     */
    private MappedFile createNewMappedFile(MappedFile oldMappedFile) {
        long startOffset = 0;
        if (oldMappedFile != null) {
            startOffset = oldMappedFile.getFileFromOffset() + this.mappedFileSize;
        }

        String nextFileName = this.storePath + File.separator + StoreUtil.offsetToFileName(startOffset);
        try {
            MappedFile mappedFile = new MappedFile(nextFileName, this.mappedFileSize);
            this.mappedFiles.add(mappedFile);
            return mappedFile;
        } catch (Exception e) {
            log.error("Error while creating new mappedFile {}, {}", nextFileName, e.getMessage());
        }
        return null;
    }


    /**
     * 根据全局物理偏移量查找对应的MappedFile
     *
     * @param offset 全局物理偏移量
     * @return 对应的MappedFile，如果找不到则返回null
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        for (MappedFile mappedFile : this.mappedFiles) {
            if (offset >= mappedFile.getFileFromOffset() &&
                    offset < mappedFile.getFileFromOffset() + this.mappedFileSize) {
                return mappedFile;
            }
        }
        return null;
    }

    /**
     * 销毁所有文件
     */
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy();
        }
        this.mappedFiles.clear();
    }

}
