package com.instaclustr.sstabletools.cassandra;

import com.instaclustr.sstabletools.AbstractSSTableReader;
import com.instaclustr.sstabletools.PartitionStatistics;
import com.instaclustr.sstabletools.SSTableStatistics;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;

/**
 * SSTable Data.db reader.
 */
public class DataReader extends AbstractSSTableReader {
    /**
     * Table scanner.
     */
    private ISSTableScanner scanner;

    /**
     * Epoch time when tombstones can be dropped.
     */
    private int gcGrace;

    /**
     * Position in Data.db file.
     */
    private long position;

    /**
     * Construct a reader for Data.db sstable file.
     *
     * @param tableStats  SSTable statistics.
     * @param scanner scanner of sstables
     * @param gcGrace gc_grace of table
     */
    public DataReader(SSTableStatistics tableStats, ISSTableScanner scanner, int gcGrace) {
        this.tableStats = tableStats;
        this.scanner = scanner;
        this.gcGrace = gcGrace;
        this.position = 0;
    }

    public boolean next() {
        if (!scanner.hasNext()) {
            scanner.close();
            return false;
        }
        UnfilteredRowIterator partition = scanner.next();
        this.partitionStats = new PartitionStatistics(partition.partitionKey());
        this.tableStats.partitionCount++;
        if (!partition.staticRow().isEmpty()) {
            Row row = partition.staticRow();
            int cellCount = row.columns().size() + row.clustering().size();
            this.partitionStats.cellCount += cellCount;
            this.tableStats.cellCount += cellCount;
        }
        if (!partition.partitionLevelDeletion().isLive()) {
            this.tableStats.partitionDeleteCount++;
        }
        while (partition.hasNext()) {
            Unfiltered unfiltered = partition.next();
            switch (unfiltered.kind()) {
                case ROW:
                    Row row = (Row) unfiltered;
                    this.partitionStats.rowCount++;
                    this.tableStats.rowCount++;
                    if (!row.deletion().isLive()) {
                        this.partitionStats.rowDeleteCount++;
                        this.tableStats.rowDeleteCount++;
                    }
                    LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
                    if (!liveInfo.isEmpty()) {
                        int ttl = liveInfo.ttl();
                        if (ttl != Cell.NO_TTL) {
                            this.partitionStats.ttl(ttl);
                        } else {
                            this.partitionStats.ttl(PartitionStatistics.NO_TTL);
                        }
                    }
                    for (Cell cell : row.cells()) {
                        this.partitionStats.cellCount++;
                        this.tableStats.cellCount++;
                        if (cell.isLive(gcGrace)) {
                            this.tableStats.liveCellCount++;
                        }
                        if (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl()) {
                            int ttl = cell.ttl();
                            if (ttl != Cell.NO_TTL) {
                                this.partitionStats.ttl(ttl);
                            } else {
                                this.partitionStats.ttl(PartitionStatistics.NO_TTL);
                            }
                        }
                        if (cell.isTombstone()) {
                            this.partitionStats.tombstoneCount++;
                            this.tableStats.tombstoneCount++;
                            if (!cell.isLive(gcGrace)) {
                                this.partitionStats.droppableTombstoneCount++;
                                this.tableStats.droppableTombstoneCount++;
                            }
                        } else if (cell.isExpiring()) {
                            this.tableStats.expiringCellCount++;
                        } else if (cell.isCounterCell()) {
                            this.tableStats.counterCellCount++;
                        }
                    }
                    break;
                case RANGE_TOMBSTONE_MARKER:
                    this.partitionStats.tombstoneCount++;
                    this.tableStats.tombstoneCount++;
                    this.tableStats.rangeTombstoneCount++;
                    break;
            }
        }
        long currentPosition = scanner.getCurrentPosition();
        this.partitionStats.size = currentPosition - position;
        position = currentPosition;
        this.tableStats.maxPartitionSize = Math.max(this.partitionStats.size, this.tableStats.maxPartitionSize);
        return true;
    }
}
