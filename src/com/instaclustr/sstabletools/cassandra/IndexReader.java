package com.instaclustr.sstabletools.cassandra;

import com.instaclustr.sstabletools.AbstractSSTableReader;
import com.instaclustr.sstabletools.PartitionStatistics;
import com.instaclustr.sstabletools.SSTableStatistics;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * SSTable Index.db reader.
 */
public class IndexReader extends AbstractSSTableReader {
    /**
     * Index.db reader.
     */
    private RandomAccessReader reader;

    /**
     * SSTable version.
     */
    private Version version;

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
     * @param reader      Reader to Index.db file.
     * @param partitioner The sstable partitioner.
     */
    public IndexReader(SSTableStatistics tableStats, RandomAccessReader reader, Version version, IPartitioner partitioner) {
        this.tableStats = tableStats;
        this.reader = reader;
        this.version = version;
        this.nextKey = null;
        this.partitioner = partitioner;
    }

    /**
     * Skip data field on index entry.
     *
     * @throws IOException
     */
    private void skipData() throws IOException {
        int size = version.storeRows() ? (int) reader.readUnsignedVInt() : reader.readInt();
        if (size > 0) {
            reader.skipBytesFully(size);
        }
    }

    @Override
    public boolean next() {
        if (completed) {
            return false;
        }
        try {
            if (nextKey == null) {
                nextKey = ByteBufferUtil.readWithShortLength(reader);
                nextPosition = version.storeRows() ? reader.readUnsignedVInt() : reader.readLong();
                skipData();
            }
            partitionStats = new PartitionStatistics(partitioner.decorateKey(nextKey));
            long position = nextPosition;
            if (!reader.isEOF()) {
                nextKey = ByteBufferUtil.readWithShortLength(reader);
                nextPosition = version.storeRows() ? reader.readUnsignedVInt() : reader.readLong();
                skipData();
                partitionStats.size = nextPosition - position;
            } else {
                partitionStats.size = this.tableStats.size - position;
                reader.close();
                completed = true;
            }
            this.tableStats.partitionCount++;
            this.tableStats.maxPartitionSize = Math.max(partitionStats.size, this.tableStats.maxPartitionSize);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            if (!completed) {
                try {
                    reader.close();
                } catch (Throwable t) {
                }
            }
            completed = true;
            return false;
        }
    }
}
