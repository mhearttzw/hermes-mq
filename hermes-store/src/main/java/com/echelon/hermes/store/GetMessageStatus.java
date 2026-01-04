package com.echelon.hermes.store;

public enum GetMessageStatus {
    FOUND,
    NO_MESSAGE_IN_QUEUE,
    OFFSET_ILLEGAL,
    OFFSET_TOO_SMALL,
}