package com.instaclustr.sstabletools;

import org.apache.cassandra.io.sstable.SSTableId;

import java.util.Comparator;

import static com.instaclustr.sstabletools.Util.compareIds;

/**
 * Metadata statistics about sstable.
 */
public class SSTableMetadata {
    public final static Comparator<SSTableMetadata> DTCS_COMPARATOR = new Comparator<SSTableMetadata>() {
        @Override
        public int compare(SSTableMetadata o1, SSTableMetadata o2) {
            int cmp = Long.compare(o1.minTimestamp, o2.minTimestamp);
            return compareIds(cmp, o1.ssTableId, o2.ssTableId);
        }
    };

    public final static Comparator<SSTableMetadata> TWCS_COMPARATOR = new Comparator<SSTableMetadata>() {
        @Override
        public int compare(SSTableMetadata o1, SSTableMetadata o2) {
            int cmp = Long.compare(o1.maxTimestamp, o2.maxTimestamp);
            return compareIds(cmp, o1.ssTableId, o2.ssTableId);
        }
    };

    public final static Comparator<SSTableMetadata> GENERATION_COMPARATOR = new Comparator<SSTableMetadata>() {
        @Override
        public int compare(SSTableMetadata o1, SSTableMetadata o2) {
            return compareIds(0, o1.ssTableId, o2.ssTableId);
        }
    };

    public final static Comparator<SSTableMetadata> LEVEL_COMPARATOR = new Comparator<SSTableMetadata>() {
        @Override
        public int compare(SSTableMetadata o1, SSTableMetadata o2) {
            int cmp = Long.compare(o1.level, o2.level);
            return compareIds(cmp, o1.ssTableId, o2.ssTableId);
        }
    };

    /**
     * File name of SSTable Data.db.
     */
    public String filename;

    /**
     * SSTable generation.
     */
    public SSTableId ssTableId;

    public long minTimestamp;

    public long maxTimestamp;

    public long minLocalDeletionTime;

    public long maxLocalDeletionTime;

    public long fileTimestamp;

    public long diskLength;

    public long uncompressedLength;

    public long keys;

    public long maxRowSize;

    public long avgRowSize;

    public long maxColumnCount;

    public long avgColumnCount;

    public double droppableTombstones;

    public boolean isRepaired;

    public long repairedAt;

    public int level;
}
