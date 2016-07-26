package io.jmnarloch.cassandra.kafka.utils;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.utils.FBUtilities;

import java.util.Collection;

public final class TriggerUtils {

    private TriggerUtils() {

    }

    public static String getKey(Partition partition) {
        return partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
    }

    public static RowIterator rowIterator(UnfilteredRowIterator iterator) {
        return UnfilteredRowIterators.filter(iterator, FBUtilities.nowInSeconds());
    }

    public static Collection<Mutation> nothing() {
        return null;
    }
}
