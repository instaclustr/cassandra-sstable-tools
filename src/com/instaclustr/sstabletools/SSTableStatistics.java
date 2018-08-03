package com.instaclustr.sstabletools;

import java.util.Comparator;

/**
 * SSTable statistics.
 */
public class SSTableStatistics {
    public final static Comparator<SSTableStatistics> LIVENESS_COMPARATOR = new Comparator<SSTableStatistics>() {
        @Override
        public int compare(SSTableStatistics o1, SSTableStatistics o2) {
            int cmp = Long.compare(o1.getLiveness(), o2.getLiveness());
            return cmp == 0 ? Integer.compare(o1.generation, o2.generation) : cmp;
        }
    };

    public final static Comparator<SSTableStatistics> DTCS_COMPARATOR = new Comparator<SSTableStatistics>() {
        @Override
        public int compare(SSTableStatistics o1, SSTableStatistics o2) {
            int cmp = Long.compare(o1.minTimestamp, o2.minTimestamp);
            return cmp == 0 ? Integer.compare(o1.generation, o2.generation) : cmp;
        }
    };

    public final static Comparator<SSTableStatistics> TWCS_COMPARATOR = new Comparator<SSTableStatistics>() {
        @Override
        public int compare(SSTableStatistics o1, SSTableStatistics o2) {
            int cmp = Long.compare(o1.maxTimestamp, o2.maxTimestamp);
            return cmp == 0 ? Integer.compare(o1.generation, o2.generation) : cmp;
        }
    };

    /**
     * SSTable generation.
     */
    public int generation;

    /**
     * File name of SSTable Data.db.
     */
    public String filename;

    /**
     * Minimum timestamp of SSTable.
     */
    public long minTimestamp;

    /**
     * Maximum timestamp of SSTable.
     */
    public long maxTimestamp;

    /**
     * LTCS sstable level.
     */
    public int level;

    /**
     * Size of SSTable in bytes.
     */
    public long size = 0;

    /**
     * Maximum partition size in bytes.
     */
    public long maxPartitionSize = 0;

    /**
     * SSTable cell count.
     */
    public long cellCount = 0;

    /**
     * SSTable live cell count.
     */
    public long liveCellCount = 0;

    /**
     * SSTable delete cell count.
     */
    public long deleteCellCount = 0;

    /**
     * SSTable expiring cell count.
     */
    public long expiringCellCount = 0;

    /**
     * SSTable range tombstone count.
     */
    public long rangeTombstoneCount = 0;

    /**
     * SSTable counter cell count.
     */
    public long counterCellCount = 0;

    /**
     * SSTable tombstone count.
     */
    public long tombstoneCount = 0;

    /**
     * SSTable droppable tombstone count.
     */
    public long droppableTombstoneCount = 0;

    /**
     * SSTable partition-level deletion count.
     */
    public long partitionDeleteCount = 0;

    /**
     * SSTable partition count.
     */
    public long partitionCount = 0;

    /**
     * Construct statistics record for SSTable.
     *
     * @param generation         SSTable Generation
     * @param filename           Filename of Data.db
     * @param uncompressedLength Uncompressed length of SSTable Data.db in bytes
     * @param minTimestamp       Minimum timestamp of SSTable
     * @param maxTimestamp       Maximum timestamp of SSTable
     * @param level              SSTable LTCS level
     */
    public SSTableStatistics(int generation, String filename, long uncompressedLength, long minTimestamp, long maxTimestamp, int level) {
        this.generation = generation;
        this.filename = filename;
        this.size = uncompressedLength;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.level = level;
    }

    /**
     * Get liveness percentage.
     *
     * @return Percentage of liveness.
     */
    public int getLiveness() {
        return (int) ((liveCellCount / (double) cellCount) * 100);
    }
}
