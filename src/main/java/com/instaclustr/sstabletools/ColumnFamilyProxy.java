package com.instaclustr.sstabletools;

import org.apache.cassandra.db.DecoratedKey;

import java.util.Collection;

/**
 * Proxy to column family related functions of Cassandra backend.
 */
public interface ColumnFamilyProxy extends AutoCloseable {

    /**
     * Get readers for SSTable Index.db files for this column family.
     *
     * @return Collection of readers for SSTable Index.db files.
     */
    Collection<SSTableReader> getIndexReaders();

    /**
     * Get readers for SSTable Data.db files for this column family.
     *
     * @return Collection of readers for SSTable Data.db files.
     */
    Collection<SSTableReader> getDataReaders();

    /**
     * Get purge statistics reader.
     *
     * @return Reader for purge statistics.
     */
    PurgeStatisticsReader getPurgeStatisticsReader();

    /**
     * Format partition key into human readable format.
     *
     * @param key Decorated partition key.
     * @return Human readable partition key.
     */
    String formatKey(DecoratedKey key);

    /**
     * Is the column family using Time Window compaction strategy.
     *
     * @return True if column family is using Time Window compaction strategy.
     */
    boolean isTWCS();

    @Override
    void close();
}
