package com.instaclustr.sstabletools.cassandra;

import com.google.common.util.concurrent.RateLimiter;
import com.instaclustr.sstabletools.PurgeStatistics;
import com.instaclustr.sstabletools.PurgeStatisticsReader;
import com.instaclustr.sstabletools.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.util.*;

/**
 * Column statistic reader backend.
 */
public class PurgeStatisticBackend implements PurgeStatisticsReader {
    /**
     * A queue of scanners sorted by key.
     */
    private PriorityQueue<ScannerWrapper> readerQueue;

    /**
     * Bytes read so far.
     */
    private long bytesRead;

    /**
     * Total uncompressed length of sstables.
     */
    private long length;

    /**
     * Time when tombstones are considered to be purgeable according to gc_grace_seconds.
     */
    private int gcBefore;

    /**
     * Column family store.
     */
    private ColumnFamilyStore cfs;

    public PurgeStatisticBackend(ColumnFamilyStore cfs, Collection<org.apache.cassandra.io.sstable.format.SSTableReader> sstables, RateLimiter rateLimiter, int gcGrace) {
        this.gcBefore = Util.NOW_SECONDS - gcGrace;
        bytesRead = 0;
        readerQueue = new PriorityQueue<>(sstables.isEmpty() ? 1 : sstables.size());
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : sstables) {
            length += sstable.uncompressedLength();
            ScannerWrapper scanner = new ScannerWrapper(sstable.descriptor.generation, sstable.getScanner(rateLimiter));
            if (scanner.next()) {
                readerQueue.add(scanner);
            }
        }
        this.cfs = cfs;
    }

    public double getProgress() {
        return bytesRead / (double) length;
    }

    @Override
    public boolean hasNext() {
        return !readerQueue.isEmpty();
    }

    @Override
    public PurgeStatistics next() {
        if (readerQueue.isEmpty()) {
            return null;
        }

        PurgeStatistics stats = new PurgeStatistics();

        // Grab row with lowest key.
        List<ScannerWrapper> scanners = new ArrayList<>(readerQueue.size());
        ScannerWrapper scanner = readerQueue.remove();
        scanners.add(scanner);
        stats.key = scanner.row.partitionKey();

        // Grab matching rows from other scanners.
        while ((scanner = this.readerQueue.peek()) != null && scanner.row.partitionKey().equals(stats.key)) {
            readerQueue.remove();
            scanners.add(scanner);
        }

        // List of rows from each scanner.
        List<UnfilteredRowIterator> rows = new ArrayList<>(scanners.size());
        for (ScannerWrapper scannerWrapper: scanners) {
            UnfilteredRowIterator row = scannerWrapper.row;
            SerializationHeader header = new SerializationHeader(false,
                    row.metadata(),
                    row.columns(),
                    row.stats());
            stats.size += serializedPartitionSize(header, row, ColumnFilter.all(cfs.metadata), MessagingService.current_version);
            row = Transformation.apply(scannerWrapper.row, new Transformation<UnfilteredRowIterator>() {
                @Override
                protected Row applyToRow(Row row) {
                    onUnfiltered(row);
                    return row;
                }

                @Override
                protected Row applyToStatic(Row row) {
                    if (row != Rows.EMPTY_STATIC_ROW) {
                        onUnfiltered(row);
                    }
                    return row;
                }

                @Override
                public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
                    onUnfiltered(marker);
                    return marker;
                }

                private void onUnfiltered(Unfiltered unfiltered) {
                    stats.size += UnfilteredSerializer.serializer.serializedSize(unfiltered, header, MessagingService.current_version);
                }
            });
            rows.add(row);
            stats.generations.add(scannerWrapper.generation);
        }

        // Merge rows together and grab column statistics.
        UnfilteredRowIterator iter = UnfilteredRowIterators.merge(rows, Util.NOW_SECONDS);
        iter = Transformation.apply(iter, new PurgeFunction(Util.NOW_SECONDS, gcBefore));
        CellCounter cellCounter = new CellCounter();
        iter = Transformation.apply(iter, cellCounter);

        long mergeSize = 0;
        if (iter.hasNext()) {
            mergeSize = serializedSize(iter, ColumnFilter.all(cfs.metadata), MessagingService.current_version);
        }

        stats.reclaimable = stats.size - mergeSize;

        // Increment scanners.
        for (ScannerWrapper scannerWrapper: scanners) {
            bytesRead += scannerWrapper.bytesRead();
            if (scannerWrapper.next()) {
                readerQueue.add(scannerWrapper);
            }
        }

        return stats;
    }

    /**
     * Return size of partition without clustering rows.
     */
    private long serializedPartitionSize(SerializationHeader header, UnfilteredRowIterator iterator, ColumnFilter selection, int version) {
        long size = ByteBufferUtil.serializedSizeWithVIntLength(iterator.partitionKey().getKey())
                + 1; // flags

        if (iterator.isEmpty()) {
            return size;
        }

        DeletionTime partitionDeletion = iterator.partitionLevelDeletion();
        Row staticRow = iterator.staticRow();
        boolean hasStatic = staticRow != Rows.EMPTY_STATIC_ROW;

        size += SerializationHeader.serializer.serializedSizeForMessaging(header, selection, hasStatic);

        if (!partitionDeletion.isLive()) {
            size += header.deletionTimeSerializedSize(partitionDeletion);
        }

        if (hasStatic) {
            size += UnfilteredSerializer.serializer.serializedSize(staticRow, header, version);
        }

        size += UnfilteredSerializer.serializer.serializedSizeEndOfPartition();

        return size;
    }

    private long serializedSize(UnfilteredRowIterator iterator, ColumnFilter selection, int version) {
        SerializationHeader header = new SerializationHeader(false,
                iterator.metadata(),
                iterator.columns(),
                iterator.stats());

        long size = serializedPartitionSize(header, iterator, selection, version);

        while (iterator.hasNext()) {
            Unfiltered unfiltered = iterator.next();
            size += UnfilteredSerializer.serializer.serializedSize(unfiltered, header, MessagingService.current_version);
        }

        return size;
    }

    public class PurgeFunction extends Transformation<UnfilteredRowIterator> {
        private final DeletionPurger purger;
        private final int nowInSec;
        private boolean isReverseOrder;

        public PurgeFunction(int nowInSec, int gcBefore) {
            this.nowInSec = nowInSec;
            this.purger = (timestamp, localDeletionTime) -> localDeletionTime < gcBefore;
        }

        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition) {
            isReverseOrder = partition.isReverseOrder();
            UnfilteredRowIterator purged = Transformation.apply(partition, this);
            if (purged.isEmpty()) {
                purged.close();
                return null;
            }
            return purged;
        }

        public DeletionTime applyToDeletion(DeletionTime deletionTime) {
            return purger.shouldPurge(deletionTime) ? DeletionTime.LIVE : deletionTime;
        }

        public Row applyToStatic(Row row) {
            return row.purge(purger, nowInSec, true);
        }

        public Row applyToRow(Row row) {
            return row.purge(purger, nowInSec, true);
        }

        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker) {
            boolean reversed = isReverseOrder;
            if (marker.isBoundary()) {
                // We can only skip the whole marker if both deletion time are purgeable.
                // If only one of them is, filterTombstoneMarker will deal with it.
                RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker) marker;
                boolean shouldPurgeClose = purger.shouldPurge(boundary.closeDeletionTime(reversed));
                boolean shouldPurgeOpen = purger.shouldPurge(boundary.openDeletionTime(reversed));

                if (shouldPurgeClose) {
                    if (shouldPurgeOpen) {
                        return null;
                    }

                    return boundary.createCorrespondingOpenMarker(reversed);
                }

                return shouldPurgeOpen
                        ? boundary.createCorrespondingCloseMarker(reversed)
                        : marker;
            } else {
                return purger.shouldPurge(((RangeTombstoneBoundMarker) marker).deletionTime()) ? null : marker;
            }
        }
    }

    private class CellCounter extends Transformation<UnfilteredRowIterator> {
        public int cellCount;

        @Override
        protected Row applyToRow(Row row) {
            onRow(row);
            return row;
        }

        @Override
        protected Row applyToStatic(Row row) {
            onRow(row);
            return row;
        }

        private void onRow(Row row) {
            cellCount += row.columns().size() + row.clustering().size();
        }
    }

    private class ScannerWrapper implements Comparable<ScannerWrapper> {
        /**
         * Generation of sstable being scanned.
         */
        public int generation;

        /**
         * SSTable scanner.
         */
        private ISSTableScanner scanner;

        /**
         * The current row.
         */
        public UnfilteredRowIterator row;

        /**
         * Position in Data.db file.
         */
        private long position;

        public ScannerWrapper(int generation, ISSTableScanner scanner) {
            this.generation = generation;
            this.scanner = scanner;
            this.position = 0;
        }

        public boolean next() {
            if (!scanner.hasNext()) {
                return false;
            }
            this.row = scanner.next();
            return true;
        }

        public long bytesRead() {
            long currentPosition = scanner.getCurrentPosition();
            long bytesRead = currentPosition - position;
            position = currentPosition;
            return bytesRead;
        }

        @Override
        public int compareTo(ScannerWrapper o) {
            return this.row.partitionKey().compareTo(o.row.partitionKey());
        }
    }

}
