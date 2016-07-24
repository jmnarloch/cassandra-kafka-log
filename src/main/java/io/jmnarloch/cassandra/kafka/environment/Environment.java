/**
 * Copyright (c) 2015-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    public Environment prefix(String prefix) {
        final Properties properties = new Properties();
        for(Map.Entry<Object, Object> entry : properties.entrySet()) {
            if(entry.getKey() instanceof String && ((String) entry.getKey()).startsWith(prefix)) {
                properties.put(((String) entry.getKey()).substring(0, prefix.length()), entry.getValue());
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
