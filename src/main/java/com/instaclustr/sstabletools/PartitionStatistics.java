package com.instaclustr.sstabletools;

import org.apache.cassandra.db.DecoratedKey;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Partition statistics.
 */
public class PartitionStatistics {
    public final static Comparator<PartitionStatistics> SIZE_COMPARATOR = new Comparator<PartitionStatistics>() {
        @Override
        public int compare(PartitionStatistics o1, PartitionStatistics o2) {
            return -Long.compare(o1.size, o2.size);
        }
    };

    public final static Comparator<PartitionStatistics> TOMBSTONE_COMPARATOR = new Comparator<PartitionStatistics>() {
        @Override
        public int compare(PartitionStatistics o1, PartitionStatistics o2) {
            int cmp = -Long.compare(o1.tombstoneCount, o2.tombstoneCount);
            return cmp == 0 ? -Long.compare(o1.size, o2.size) : cmp;
        }
    };

    public final static Comparator<PartitionStatistics> WIDE_COMPARATOR = new Comparator<PartitionStatistics>() {
        @Override
        public int compare(PartitionStatistics o1, PartitionStatistics o2) {
            int cmp = -Long.compare(o1.cellCount, o2.cellCount);
            return cmp == 0 ? -Long.compare(o1.size, o2.size) : cmp;
        }
    };

    public final static Comparator<PartitionStatistics> SSTABLE_COUNT_COMPARATOR = new Comparator<PartitionStatistics>() {
        @Override
        public int compare(PartitionStatistics o1, PartitionStatistics o2) {
            int cmp = -Long.compare(o1.tableCount, o2.tableCount);
            return cmp == 0 ? -Long.compare(o1.size, o2.size) : cmp;
        }
    };

    public final static Comparator<PartitionStatistics> MOST_DELETED_ROWS_COMPARATOR = new Comparator<PartitionStatistics>() {
        @Override
        public int compare(PartitionStatistics o1, PartitionStatistics o2) {
            int cmp = -Long.compare(o1.rowDeleteCount, o2.rowDeleteCount);
            return cmp == 0 ? -Long.compare(o1.size, o2.size) : cmp;
        }
    };

    /**
     * The partition key.
     */
    public DecoratedKey key;

    /**
     * Number of sstables partition belongs to.
     */
    public int tableCount = 1;

    /**
     * Size in bytes of current partition.
     */
    public long size = 0;

    /**
     * Partition row count.
     */
    public long rowCount = 0;

    /**
     * Deleted row count.
     */
    public long rowDeleteCount = 0;

    /**
     * Partition cell count.
     */
    public long cellCount = 0;

    /**
     * Partition tombstone count (delete cells and range tombstones).
     */
    public long tombstoneCount = 0;

    /**
     * Partition droppable tombstone count.
     */
    public long droppableTombstoneCount = 0;

    public final static int NO_TTL = -1;

    public Map<Integer,Long> ttl = new HashMap<>();

    private static final Long ZERO = 0L;

    public void ttl(int key) {
        long val = this.ttl.getOrDefault(key, ZERO);
        this.ttl.put(key, val + 1);
    }

    /**
     * Construct partition statistics.
     *
     * @param key Partition key.
     */
    public PartitionStatistics(DecoratedKey key) {
        this.key = key;
    }

    /**
     * Collate this partition statistics with another partition statistics.
     *
     * @param p PartitionStatistics stats to collate with.
     * @return Collated partition stats.
     */
    public PartitionStatistics collate(PartitionStatistics p) {
        PartitionStatistics result = new PartitionStatistics(this.key);
        result.tableCount = this.tableCount + p.tableCount;
        result.size = this.size + p.size;
        result.rowCount = this.rowCount + p.rowCount;
        result.rowDeleteCount = this.rowDeleteCount + p.rowDeleteCount;
        result.cellCount = this.cellCount + p.cellCount;
        result.tombstoneCount = this.tombstoneCount + p.tombstoneCount;
        result.droppableTombstoneCount = this.droppableTombstoneCount + p.droppableTombstoneCount;
        result.ttl  = new HashMap<>(p.ttl);
        mergeTtl(result.ttl);
        return result;
    }

    public void mergeTtl(Map<Integer, Long> map) {
        for (Map.Entry<Integer,Long> entry : this.ttl.entrySet()) {
            Integer key = entry.getKey();
            Long val = entry.getValue() + map.getOrDefault(key, ZERO);
            map.put(key, val);
        }
    }
}
