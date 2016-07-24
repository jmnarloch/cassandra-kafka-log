package io.jmnarloch.cassandra.kafka.exception;

public class EnvironmentException extends RuntimeException {

    public EnvironmentException(String message) {
        super(message);
    }

    public EnvironmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
