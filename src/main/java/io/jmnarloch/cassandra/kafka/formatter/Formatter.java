package io.jmnarloch.cassandra.kafka.formatter;

import org.apache.cassandra.db.rows.Row;

public interface Formatter {

    byte[] format(Row row);
}
