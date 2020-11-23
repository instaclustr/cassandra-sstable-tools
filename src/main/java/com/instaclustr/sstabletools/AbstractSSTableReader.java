package com.instaclustr.sstabletools;

/**
 * Base reader.
 */
public abstract class AbstractSSTableReader implements SSTableReader {
    /**
     * The current partition statistics.
     */
    protected PartitionStatistics partitionStats;

    /**
     * Statistics for SSTable.
     */
    protected SSTableStatistics tableStats;

    @Override
    public PartitionStatistics getPartitionStatistics() {
        return partitionStats;
    }

    @Override
    public SSTableStatistics getSSTableStatistics() {
        return tableStats;
    }

    @Override
    public int compareTo(SSTableReader o) {
        return this.partitionStats.key.compareTo(((AbstractSSTableReader) o).partitionStats.key);
    }
}
