package com.instaclustr.sstabletools.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.instaclustr.sstabletools.AbstractSSTableReader;
import com.instaclustr.sstabletools.PartitionStatistics;
import com.instaclustr.sstabletools.SSTableStatistics;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.KeyReader;

/**
 * SSTable Index.db reader.
 */
public class IndexReader extends AbstractSSTableReader {

    /**
     * The SSTable KeyReader.
     */
    private KeyReader keyReader;

    /**
     * The sstable partitioner.
     */
    private IPartitioner partitioner;

    /**
     * The next partition key.
     */
    private ByteBuffer nextKey;

    /**
     * The position in Data.db of the following partition key.
     */
    private long nextPosition;

    /**
     * Flag to determine that the last index entry has been read.
     */
    private boolean completed = false;


    /**
     * Construct a reader for Index.db sstable file.
     *
     * @param tableStats  SSTable statistics.
     * @param keyReader   KeyReader for sstable.
     * @param partitioner The sstable partitioner.
     */
    public IndexReader(SSTableStatistics tableStats, KeyReader keyReader, IPartitioner partitioner) {
        this.tableStats = tableStats;
        this.keyReader = keyReader;
        this.nextKey = null;
        this.partitioner = partitioner;
    }

    @Override
    public boolean next() {
        if (completed) {
            return false;
        }
        try {
            if (nextKey == null) {
                nextKey = keyReader.key();
                nextPosition = keyReader.dataPosition();
            }
            partitionStats = new PartitionStatistics(partitioner.decorateKey(nextKey));
            long position = nextPosition;
            if (!keyReader.isExhausted() && keyReader.advance()) {
                nextKey = keyReader.key();
                nextPosition = keyReader.dataPosition();
                partitionStats.size = nextPosition - position;
            } else {
                partitionStats.size = this.tableStats.size - position;
                keyReader.close();
                completed = true;
            }
            this.tableStats.partitionCount++;
            this.tableStats.maxPartitionSize = Math.max(partitionStats.size, this.tableStats.maxPartitionSize);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            if (!completed) {
                try {
                    keyReader.close();
                } catch (Throwable t) {
                }
            }
            completed = true;
            return false;
        }
    }
}
