package com.instaclustr.sstabletools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Read partitions from collection of SSTables.
 */
public class PartitionReader {
    /**
     * Statistics for each SSTable.
     */
    private List<SSTableStatistics> sstableStats;

    /**
     * Bytes read.
     */
    private long bytesRead;

    /**
     * Length in bytes to read.
     */
    private long length;

    /**
     * A queue of DataReader sorted by key.
     */
    private PriorityQueue<SSTableReader> readerQueue;

    /**
     * Constructor.
     *
     * @param readers Collection of SSTable readers.
     * @param length  Total length in bytes to be read.
     */
    public PartitionReader(Collection<SSTableReader> readers, long length) {
        for (SSTableReader reader : readers) {
            reader.next();
        }
        this.readerQueue = new PriorityQueue<>(readers);
        this.sstableStats = new ArrayList<>(readers.size());
        this.length = length;
    }

    /**
     * Gets the next partition from the collection of sstables.
     * <p>
     * Since sstables are sorted by partition key we can process the sstables in a single iteration pass by reading
     * the smallest partition key from the collection of sstables.
     *
     * @return Partition statistics or null if no partitions left to read.
     */
    public PartitionStatistics read() {
        if (this.readerQueue.isEmpty()) {
            return null;
        }

        SSTableReader reader = this.readerQueue.remove();
        PartitionStatistics pStats = reader.getPartitionStatistics();
        readerNext(reader);
        // Combine entries with matching key
        while ((reader = this.readerQueue.peek()) != null && reader.getPartitionStatistics().key.equals(pStats.key)) {
            this.readerQueue.remove();
            pStats = pStats.collate(reader.getPartitionStatistics());
            readerNext(reader);
        }
        this.bytesRead += pStats.size;
        return pStats;
    }

    /**
     * Goto next partition in reader. If finished processing the sstable collect its statistics.
     *
     * @param reader SSTable Data.db reader.
     */
    private void readerNext(SSTableReader reader) {
        // If index is not finished add back to queue.
        if (reader.next()) {
            this.readerQueue.add(reader);
        } else { // Otherwise collect the table statistics.
            this.sstableStats.add(reader.getSSTableStatistics());
        }
    }

    /**
     * Read progress.
     *
     * @return Read progress as percentage.
     */
    public double getProgress() {
        return bytesRead / (double) length;
    }

    /**
     * Get SSTable statistics.
     *
     * @return Statistics for each SSTable.
     */
    public List<SSTableStatistics> getSSTableStatistics() {
        return this.sstableStats;
    }
}
