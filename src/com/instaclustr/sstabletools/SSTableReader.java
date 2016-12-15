package com.instaclustr.sstabletools;

/**
 * SSTable reader.
 */
public interface SSTableReader extends Comparable<SSTableReader> {
    /**
     * Get the next partition.
     *
     * @return True if a partition was retrieved.
     */
    boolean next();

    /**
     * Get the partition statistics.
     *
     * @return Partition statistics.
     */
    PartitionStatistics getPartitionStatistics();

    /**
     * Get the SSTable statistics.
     *
     * @return SSTable statistics.
     */
    SSTableStatistics getSSTableStatistics();
}
