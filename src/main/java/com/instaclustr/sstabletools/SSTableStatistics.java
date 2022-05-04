package com.instaclustr.sstabletools;

import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;

import java.util.Comparator;

/**
 * SSTable statistics.
 */
public class SSTableStatistics {

    public final static Comparator<SSTableStatistics> LIVENESS_COMPARATOR = new Comparator<SSTableStatistics>() {
        @Override
        public int compare(SSTableStatistics o1, SSTableStatistics o2) {
            int cmp = Long.compare(o1.getLiveness(), o2.getLiveness());
            return Util.compareIds(cmp, o1.ssTableId, o2.ssTableId);
        }
    };

    public final static Comparator<SSTableStatistics> DTCS_COMPARATOR = new Comparator<SSTableStatistics>() {
        @Override
        public int compare(SSTableStatistics o1, SSTableStatistics o2) {
            int cmp = Long.compare(o1.minTimestamp, o2.minTimestamp);
            return Util.compareIds(cmp, o1.ssTableId, o2.ssTableId);
        }
    };

    public final static Comparator<SSTableStatistics> TWCS_COMPARATOR = new Comparator<SSTableStatistics>() {
        @Override
        public int compare(SSTableStatistics o1, SSTableStatistics o2) {
            int cmp = Long.compare(o1.maxTimestamp, o2.maxTimestamp);
            return Util.compareIds(cmp, o1.ssTableId, o2.ssTableId);
        }
    };

    /**
     * SSTable id.
     */
    public SSTableId ssTableId;

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
     * Partition row count.
     */
    public long rowCount = 0;

    /**
     * Deleted row count.
     */
    public long rowDeleteCount = 0;

    /**
     * SSTable cell count.
     */
    public long cellCount = 0;

    /**
     * SSTable live cell count.
     */
    public long liveCellCount = 0;

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
     * @param ssTableId         SSTable id
     * @param filename           Filename of Data.db
     * @param uncompressedLength Uncompressed length of SSTable Data.db in bytes
     * @param minTimestamp       Minimum timestamp of SSTable
     * @param maxTimestamp       Maximum timestamp of SSTable
     * @param level              SSTable LTCS level
     */
    public SSTableStatistics(SSTableId ssTableId, String filename, long uncompressedLength, long minTimestamp, long maxTimestamp, int level) {
        this.ssTableId = ssTableId;
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
