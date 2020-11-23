package com.instaclustr.sstabletools;

import org.apache.cassandra.db.DecoratedKey;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Column statistics about a partition.
 */
public class PurgeStatistics {
    public final static Comparator<PurgeStatistics> PURGE_COMPARATOR = new Comparator<PurgeStatistics>() {
        @Override
        public int compare(PurgeStatistics o1, PurgeStatistics o2) {
            int cmp = -Long.compare(o1.reclaimable, o2.reclaimable);
            return cmp == 0 ? -Long.compare(o1.size, o2.size) : cmp;
        }
    };

    /**
     * The partition key.
     */
    public DecoratedKey key;

    /**
     * List of generations the key belongs to.
     */
    public List<Integer> generations = new ArrayList<>();

    /**
     * Size in bytes of current partition.
     */
    public long size = 0;

    /**
     * Size in bytes reclaimed after compaction.
     */
    public long reclaimable = 0;
}
