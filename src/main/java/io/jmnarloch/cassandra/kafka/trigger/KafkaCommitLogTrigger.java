/**
 * Copyright (c) 2015-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jmnarloch.cassandra.kafka.trigger;

import io.jmnarloch.cassandra.kafka.api.CommitLog;
import io.jmnarloch.cassandra.kafka.environment.Environment;
import io.jmnarloch.cassandra.kafka.exception.CommitLogException;
import io.jmnarloch.cassandra.kafka.formatter.Formatter;
import io.jmnarloch.cassandra.kafka.formatter.FormatterFactory;
import io.jmnarloch.cassandra.kafka.infrastructure.KafkaCommitLog;
import io.jmnarloch.cassandra.kafka.row.RowInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.triggers.ITrigger;

import java.util.Collection;

import static io.jmnarloch.cassandra.kafka.utils.CassandraUtils.getKey;
import static io.jmnarloch.cassandra.kafka.utils.CassandraUtils.nothing;
import static io.jmnarloch.cassandra.kafka.utils.CassandraUtils.rowIterator;

public class KafkaCommitLogTrigger implements ITrigger {

    private final Environment environment;

    private final Formatter formatter;

    public KafkaCommitLogTrigger() {
        this(Environment.loadDefault());
    }

    protected KafkaCommitLogTrigger(Environment environment) {
        this.environment = environment;
        this.formatter = FormatterFactory.createFormatter(environment);
    }

    @Override
    public Collection<Mutation> augment(Partition update) {

        try (final CommitLog commitLog = new KafkaCommitLog(environment)) {
            final String key = getKey(update);
            final RowIterator rows = rowIterator(update.unfilteredIterator());
            while (rows.hasNext()) {
                final byte[] data = formatter.format(
                        new RowInfo(update.metadata(), update.partitionKey(), rows.next())
                );
                commitLog.commit(key, data);
            }
            return nothing();
        } catch (Exception e) {
            throw new CommitLogException("An unexpected error occurred when trying to export the row data", e);
        }
    }
}
