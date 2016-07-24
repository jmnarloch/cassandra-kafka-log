package io.jmnarloch.cassandra.kafka.infrastructure;

import io.jmnarloch.cassandra.kafka.api.CommitLog;
import io.jmnarloch.cassandra.kafka.environment.Environment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaCommitLog implements CommitLog {

    private final String topic;

    private final KafkaProducer<String, byte[]> kafkaProducer;

    public KafkaCommitLog(Environment environment) {
        this.topic = environment.get("kafka.topic");
        this.kafkaProducer = new KafkaProducer<>(environment.filter("kafka.").asProperties());
    }

    @Override
    public void commit(String key, byte[] data) {
        kafkaProducer.send(new ProducerRecord<>(topic, key, data));
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}
