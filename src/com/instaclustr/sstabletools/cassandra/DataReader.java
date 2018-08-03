package com.instaclustr.sstabletools.cassandra;

import com.instaclustr.sstabletools.AbstractSSTableReader;
import com.instaclustr.sstabletools.PartitionStatistics;
import com.instaclustr.sstabletools.SSTableStatistics;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;

/**
 * SSTable Data.db reader.
 */
public class DataReader extends AbstractSSTableReader {
    /**
     * Table scanner.
     */
    private SSTableScanner scanner;

    /**
     * Epoch time when tombstones can be dropped.
     */
    private int gcGrace;

    /**
     * Position in Data.db file.
     */
    private long position;

    /**
     * Construct a reader for Index.db sstable file.
     *
     * @param tableStats  SSTable statistics.
     */
    public DataReader(SSTableStatistics tableStats, SSTableScanner scanner, int gcGrace) {
        this.tableStats = tableStats;
        this.scanner = scanner;
        this.gcGrace = gcGrace;
        this.position = 0;
    }

    @Override
    public boolean next() {
        if (!scanner.hasNext()) {
            try {
                scanner.close();
            } catch (Throwable t) {}
            return false;
        }
        SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
        this.partitionStats = new PartitionStatistics(row.getKey());
        DeletionInfo deletionInfo = row.getColumnFamily().deletionInfo();
        this.tableStats.partitionCount++;
        if (!deletionInfo.getTopLevelDeletion().isLive()) {
            this.tableStats.partitionDeleteCount++;
        }
        this.partitionStats.cellCount = 0;
        this.partitionStats.tombstoneCount = 0;
        while (row.hasNext()) {
            OnDiskAtom atom = row.next();
            if (atom instanceof Column) {
                Column cell = (Column) atom;
                this.partitionStats.cellCount++;
                this.tableStats.cellCount++;
                if (cell.isLive(gcGrace)) {
                    this.tableStats.liveCellCount++;
                }
            }
            if (atom instanceof DeletedColumn) {
                this.tableStats.deleteCellCount++;
                this.partitionStats.tombstoneCount++;
                this.tableStats.tombstoneCount++;
                if (atom.getLocalDeletionTime() < gcGrace) {
                    this.partitionStats.droppableTombstoneCount++;
                    this.tableStats.droppableTombstoneCount++;
                }
            } else if (atom instanceof ExpiringColumn) {
                this.tableStats.expiringCellCount++;
            } else if (atom instanceof CounterColumn) {
                this.tableStats.counterCellCount++;
            } else if (atom instanceof RangeTombstone) {
                this.tableStats.rangeTombstoneCount++;
                this.partitionStats.tombstoneCount++;
                this.tableStats.tombstoneCount++;
                if (atom.getLocalDeletionTime() < gcGrace) {
                    this.partitionStats.droppableTombstoneCount++;
                    this.tableStats.droppableTombstoneCount++;
                }
            }
        }
        long currentPosition = scanner.getCurrentPosition();
        this.partitionStats.size = currentPosition - position;
        position = currentPosition;
        this.tableStats.maxPartitionSize = Math.max(this.partitionStats.size, this.tableStats.maxPartitionSize);
        return true;
    }
}
