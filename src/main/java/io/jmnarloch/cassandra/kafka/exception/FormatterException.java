package io.jmnarloch.cassandra.kafka.exception;

public class FormatterException extends RuntimeException {

    public FormatterException(String message) {
        super(message);
    }

    public FormatterException(String message, Throwable cause) {
        super(message, cause);
    }
}
