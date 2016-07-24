package io.jmnarloch.cassandra.kafka.api;

public interface CommitLog extends AutoCloseable {

    void commit(String key, byte[] data);
}
