package com.echelon.hermes.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

/**
 * 描述：对单个内存映射文件的封装。
 * 这是CommitLog和ConsumeQueue的基础存储单元。
 *
 * @author jorelwang
 * @create 2025-09-21 12:36
 */
public class MappedFile {

    private static final Logger log = LoggerFactory.getLogger(MappedFile.class);


    // 单个文件的大小，例如1GB
    private final int fileSize;

    // 文件对应的FileChannel
    // rocketMq中这里没有关键字final，是因为使用了transientStorePool（瞬态存储池）
    // 预分配内存+延迟绑定
    // 在Broker启动时，RocketMQ可以预先在**堆外内存（Direct ByteBuffer Pool）**中分配一块巨大的内存池，而不是等到需要创建MappedFile时再去向操作系统申请内存映射。
    private final FileChannel fileChannel;

    // 文件名，通常是该文件第一条消息在整个队列中的起始物理偏移量
    private final String fileName;

    // 文件的起始偏移量
    private final long fileFromOffset;

    // 文件对象
    private final File file;

    // 核心的内存映射缓冲区
    private final MappedByteBuffer mappedByteBuffer;

    // 当前文件已经写入的位置
    // wrotePosition会有刷盘线程（Flush Thread）进行读取，所以需要保持可见性，设置为AtomicInteger
    private final AtomicInteger wrotePosition;

    // 已经刷盘的位置
    private final AtomicInteger flushedPosition;

    private static final Unsafe UNSAFE;

    static {
        UNSAFE = getUnsafe();
    }

    /**
     * 【新方法】
     * 这是一个专门用于获取Unsafe实例的私有静态方法。
     * 将复杂的初始化逻辑封装起来，可以保证final字段的安全初始化，
     * 并让static块保持整洁。
     *
     * @return a Unsafe instance, or null if failed.
     */
    private static Unsafe getUnsafe() {
        try {
            // Unsafe 的构造函数是私有的，只能通过反射 a "theUnsafe" 字段获取
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (Unsafe) unsafeField.get(null);
        } catch (Exception e) {
            log.error("Failed to get Unsafe instance. Unmap mapped file may not work.", e);
            return null;
        }
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());

        boolean ok = false;
        // 确保父目录存在
        ensureDirOK(this.file.getParent());

        // 声明临时变量来持有将要赋给final字段的值
        FileChannel tempFileChannel = null;
        MappedByteBuffer tempMappedByteBuffer = null;

        try {
            // 1、以读写方式创建文件通道
            tempFileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 2、获取文件内存映射缓冲区
            tempMappedByteBuffer = tempFileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file {}", this.fileName, e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.error("Failed to map file {}", this.fileName, e);
            throw new RuntimeException(e);
        } finally {
            // 3. 将临时变量的值赋给final字段
            //    这一步总是在try块之后、任何异常抛出之前执行
            this.fileChannel = tempFileChannel;
            this.mappedByteBuffer = tempMappedByteBuffer;

            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }

        // 初始化为0
        this.wrotePosition = new AtomicInteger(0);
        this.flushedPosition = new AtomicInteger(0);

    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info("{} mkdir {}", dirName, result ? "OK" : "Failed");
            }
        }
    }


    /**
     * 追加消息
     * @param data  要写入的数据
     * @return      是否成功写入
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        // 检查是否有足够的空间
        // 先写入数据，再进行位置更新，这样能够保证数据恢复的一致性，避免脏数据污染
        if (currentPos + data.length <=  this.fileSize) {
            // 1. 获取一个临时的ByteBuffer切片，从当前写入位置开始
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);

            // 2. 将数据写入到这个切片中
            byteBuffer.put(data);

            // 3. 只有在数据成功写入后，才原子地更新全局的写入位置
            this.wrotePosition.addAndGet(data.length);
            return true;
        }
        // 如果空间不足，返回false
        return false;
    }

    /**
     * 将内存中的数据刷盘
     * @return  返回本次刷盘后的位置
     */
    public int flush() {
        int value = this.wrotePosition.get();
        if (value > this.flushedPosition.get()) {
            // 使用 FileChannel 的 force 方法进行刷盘
            try {
                this.fileChannel.force(false);
            } catch (Exception e) {
                log.error("Error occurred when force data to disk.", e);
            }

            // 只有在force()成功后，才更新flushedPosition
            this.flushedPosition.set(value);
        }
        return this.flushedPosition.get();
    }

    /**
     * 从文件的指定位置读取指定大小的数据。
     * @param pos  读取的起始位置 (文件内的相对位置)
     * @param size 读取的大小
     * @return 一个包含所读取数据的ByteBuffer
     */
    public ByteBuffer selectMappedBuffer(int pos, int size) {
        if (pos < this.fileSize && pos + size <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(pos);
            ByteBuffer byteBufferNew = byteBuffer.slice();
            byteBufferNew.limit(size);
            return byteBufferNew;
        }
        return null;
    }

    /**
     * 安全地销毁文件资源，包括 unmap buffer 和关闭 channel。
     */
    public void destroy() {
        if (this.fileChannel != null) {
            this.flush();
            try {
                // 安全地 unmap MappedByteBuffer
                unmap(this.mappedByteBuffer);
                // 关闭文件通道
                this.fileChannel.close();
            } catch (Exception e) {
                log.error("Error occurred when closing file:{} channel.", this.fileName, e);
            }
        }
    }

    /**
     * 支持java8等旧版本
     * 通过反射调用cleaner方法来释放MappedByteBuffer占用的内存。
     * 这是处理内存映射文件释放问题的标准“黑科技”。
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }



    /**
     * 支持java9+版本
     */
    public static void unmap(final ByteBuffer byteBuffer) {
        if (byteBuffer != null) {
            return;
        }

        AccessController.doPrivileged(new PrivilegedAction<Object>() {

            /**
             * Performs the computation.  This method will be called by
             * {@code AccessController.doPrivileged} after enabling privileges.
             *
             * @return a class-dependent value that may represent the results of the
             *         computation. Each class that implements
             *         {@code PrivilegedAction}
             *         should document what (if anything) this value represents.
             * @see AccessController#doPrivileged(PrivilegedAction)
             * @see AccessController#doPrivileged(PrivilegedAction,
             *         AccessControlContext)
             */
            @Override
            public Void run() {
                try {
                    if (UNSAFE != null) {
                        UNSAFE.invokeCleaner(byteBuffer);
                        log.info("cleaned {} unmapped bytes", byteBuffer.capacity());
                    } else {
                        log.error("Unable to invoke unmapped ByteBuffer");
                    }
                } catch (Exception e) {
                    log.error("Error occurred when trying to unmap ByteBuffer", e);
                }

                return null;
            }
        });
    }

    // --- Getters ---
    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setWrotePosition(int position) {
        wrotePosition.set(position);
    }

    public void setFlushedPosition(int position) {
        flushedPosition.set(position);
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getFileSize() {
        return fileSize;
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }


}
