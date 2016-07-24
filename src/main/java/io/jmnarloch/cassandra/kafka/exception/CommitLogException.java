package io.jmnarloch.cassandra.kafka.exception;

public class CommitLogException extends RuntimeException {

    public CommitLogException(String message) {
        super(message);
    }

    public CommitLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
