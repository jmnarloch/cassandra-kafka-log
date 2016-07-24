package io.jmnarloch.cassandra.kafka.formatter;

import io.jmnarloch.cassandra.kafka.environment.Environment;

public class FormatterFactory {

    public static Formatter createFormatter(Environment environment) {
        return new JacksonFormatter();
    }
}
