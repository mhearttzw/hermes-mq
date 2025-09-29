package com.echelon.hermes.store;

public class PutMessageResult {
        private PutMessageStatus status;
        // 消息的起始偏移量
        private long offset;
        public PutMessageResult(PutMessageStatus status, long offset) {
            this.status = status;
            this.offset = offset;
        }

        public PutMessageStatus getStatus() {
            return status;
        }

        public long getOffset() {
            return offset;
        }

    }