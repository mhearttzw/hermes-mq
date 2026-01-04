package com.echelon.hermes.protocol;

/**
 * 描述：拉取消息响应-自定义业务Header
 *
 * @author jorelwang
 * @create 2025-10-11 17:26
 */
public class PullMessageResponseHeader implements CommandCustomHeader {

    /**
     * broker建议的、消费者下一次应该开始拉取的偏移量
     */
    private Long nextBeginOffset;

    private Long minOffset;

    private Long maxOffset;


    // --- Getters and Setters ---
    public Long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(Long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

}
