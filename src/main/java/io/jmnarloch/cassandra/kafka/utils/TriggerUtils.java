package io.jmnarloch.cassandra.kafka.utils;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Unfiltered;

import java.util.Collection;

public final class TriggerUtils {

    private TriggerUtils() {

    }

    public static String getKey(Partition partition) {
        return partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
    }

    public static boolean shouldSkip(Unfiltered unfiltered) {
        return !unfiltered.isRow() || !Clustering.class.isAssignableFrom(unfiltered.getClass());
    }

    public static Collection<Mutation> nothing() {
        return null;
    }
}
