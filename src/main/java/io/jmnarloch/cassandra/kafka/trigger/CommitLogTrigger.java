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
package io.jmnarloch.cassandra.kafka.trigger;

import io.jmnarloch.cassandra.kafka.api.CommitLog;
import io.jmnarloch.cassandra.kafka.environment.Environment;
import io.jmnarloch.cassandra.kafka.exception.CommitLogException;
import io.jmnarloch.cassandra.kafka.formatter.Formatter;
import io.jmnarloch.cassandra.kafka.formatter.FormatterFactory;
import io.jmnarloch.cassandra.kafka.infrastructure.KafkaCommitLog;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;

import java.util.Collection;

public class CommitLogTrigger implements ITrigger {

    @Override
    public Collection<Mutation> augment(Partition update) {

        final Environment environment = Environment.loadDefault();
        final Formatter formatter = FormatterFactory.createFormatter(environment);
        try (final CommitLog commitLog = new KafkaCommitLog(environment)) {
            final String key = update.metadata().getKeyValidator().getString(update.partitionKey().getKey());
            final UnfilteredRowIterator rows = update.unfilteredIterator();
            while (rows.hasNext()) {
                final Unfiltered unfilteredRow = rows.next();
                if (shouldSkip(unfilteredRow)) {
                    continue;
                }
                final Row row = update.getRow((Clustering) unfilteredRow);
                final byte[] data = formatter.format(row);
                commitLog.commit(key, data);
            }
            return nothing();
        } catch (Exception e) {
            throw new CommitLogException("An unexpected error occurred when trying to export the column information", e);
        }
    }

    private boolean shouldSkip(Unfiltered unfiltered) {
        return !unfiltered.isRow() || !Clustering.class.isAssignableFrom(unfiltered.getClass());
    }

    private static Collection<Mutation> nothing() {
        return null;
    }
}
