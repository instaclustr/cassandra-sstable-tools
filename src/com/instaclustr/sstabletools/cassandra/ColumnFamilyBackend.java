package com.instaclustr.sstabletools.cassandra;

import com.google.common.util.concurrent.RateLimiter;
import com.instaclustr.sstabletools.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ColumnFamilyProxy using Cassandra 2.0 backend.
 */
public class ColumnFamilyBackend implements ColumnFamilyProxy {
    /**
     * Key validator for column family.
     */
    private AbstractType<?> keyValidator;

    /**
     * Is column family using Date Tiered Compaction Strategy.
     */
    private boolean isDTCS;

    /**
     * Column family store.
     */
    private ColumnFamilyStore cfStore;

    /**
     * Snapshot name.
     */
    private String snapshotName;

    /**
     * Track if snapshot is to be cleared.
     */
    private boolean clearSnapshot;

    /**
     * Collection of SSTables.
     */
    private Collection<org.apache.cassandra.io.sstable.SSTableReader> sstables;

    public ColumnFamilyBackend(AbstractType<?> keyValidator, boolean isDTCS, ColumnFamilyStore cfStore, String snapshotName, Collection<String> filter) throws IOException {
        this.keyValidator = keyValidator;
        this.isDTCS = isDTCS;
        this.cfStore = cfStore;
        if (snapshotName != null) {
            this.clearSnapshot = false;
        } else {
            snapshotName = Util.generateSnapshotName();
            cfStore.snapshotWithoutFlush(snapshotName);
            this.clearSnapshot = true;
        }
        this.snapshotName = snapshotName;
        this.sstables = cfStore.getSnapshotSSTableReader(snapshotName);
        if (filter != null) {
            List<org.apache.cassandra.io.sstable.SSTableReader> filteredSSTables = new ArrayList<>(sstables.size());
            for (org.apache.cassandra.io.sstable.SSTableReader sstable : sstables) {
                File dataFile = new File(sstable.descriptor.filenameFor(Component.DATA));
                if (filter.contains(dataFile.getName())) {
                    filteredSSTables.add(sstable);
                }
            }
            this.sstables = filteredSSTables;
        }
    }

    @Override
    public Collection<SSTableReader> getIndexReaders() {
        Collection<SSTableReader> readers = new ArrayList<>(sstables.size());
        for (org.apache.cassandra.io.sstable.SSTableReader sstable : sstables) {
            try {
                File dataFile = new File(sstable.descriptor.filenameFor(Component.DATA));
                readers.add(new IndexReader(
                        new SSTableStatistics(
                                sstable.descriptor.generation,
                                dataFile.getName(),
                                sstable.uncompressedLength(),
                                sstable.getMinTimestamp(),
                                sstable.getMaxTimestamp(),
                                sstable.getSSTableLevel()),
                        sstable.openIndexReader(),
                        sstable.partitioner
                ));
            } catch (Throwable t) {
            }
        }
        return readers;
    }

    @Override
    public Collection<SSTableReader> getDataReaders(RateLimiter rateLimiter) {
        Collection<SSTableReader> readers = new ArrayList<>(sstables.size());
        for (org.apache.cassandra.io.sstable.SSTableReader sstable : sstables) {
            try {
                File dataFile = new File(sstable.descriptor.filenameFor(Component.DATA));
                readers.add(new DataReader(
                        new SSTableStatistics(
                                sstable.descriptor.generation,
                                dataFile.getName(),
                                sstable.uncompressedLength(),
                                sstable.getMinTimestamp(),
                                sstable.getMaxTimestamp(),
                                sstable.getSSTableLevel()),
                        sstable.getScanner(rateLimiter),
                        Util.NOW_SECONDS - sstable.metadata.getGcGraceSeconds()
                ));
            } catch (Throwable t) {
            }
        }
        return readers;
    }

    @Override
    public PurgeStatisticsReader getPurgeStatisticsReader(RateLimiter rateLimiter) {
        return new PurgeStatisticBackend(cfStore, sstables, rateLimiter);
    }

    @Override
    public String formatKey(DecoratedKey key) {
        return keyValidator.getString(key.key);
    }

    @Override
    public boolean isDTCS() {
        return isDTCS;
    }

    @Override
    public void close() {
        if (clearSnapshot) {
            cfStore.clearSnapshot(snapshotName);
            clearSnapshot = false;
        }
    }
}
