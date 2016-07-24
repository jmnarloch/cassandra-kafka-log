package io.jmnarloch.cassandra.kafka.environment;

import io.jmnarloch.cassandra.kafka.exception.EnvironmentException;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class Environment {

    private static final String DEFAULT_PROPERTIES = "commit-log.properties";

    private final Properties properties;

    private Environment(Properties properties) {
        this.properties = properties;
    }

    public String get(String key) {
        return (String) properties.get(key);
    }

    public Environment filter(String prefix) {
        final Properties properties = new Properties();
        for(Map.Entry<Object, Object> entry : properties.entrySet()) {
            if(entry.getKey() instanceof String && ((String) entry.getKey()).startsWith(prefix)) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }
        return new Environment(properties);
    }

    public Properties asProperties() {
        return new Properties(properties);
    }

    public static Environment loadDefault() {
        try (final InputStream input = open(DEFAULT_PROPERTIES)) {
            Properties properties = new Properties();
            properties.load(input);
            return new Environment(properties);
        } catch (Exception e) {
            throw new EnvironmentException("Commit Log properties could not be loaded.", e);
        }
    }

    private static InputStream open(String file) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    }
}
