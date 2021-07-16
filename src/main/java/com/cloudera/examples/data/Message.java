package com.cloudera.examples.data;

public class Message {
    public Integer hash;
    public Long timestamp;

    public Message() {}

    public Message(Integer hash, Long timestamp) {
        this.hash = hash;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("Message{%s, %s}", hash, timestamp);
    }
}
