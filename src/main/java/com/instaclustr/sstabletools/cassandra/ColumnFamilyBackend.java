package com.instaclustr.sstabletools.cassandra;

import com.instaclustr.sstabletools.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * ColumnFamilyProxy using Cassandra 3.5 backend.
 */
public class ColumnFamilyBackend implements ColumnFamilyProxy {

    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyBackend.class);

    /**
     * Key validator for column family.
     */
    private AbstractType<?> keyValidator;

    /**
     * Is column family using Time Window Compaction Strategy.
     */
    private boolean isTWCS;

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
    private Collection<org.apache.cassandra.io.sstable.format.SSTableReader> sstables;

    public ColumnFamilyBackend(AbstractType<?> keyValidator,
                               boolean isTWCS,
                               ColumnFamilyStore cfStore,
                               String snapshotName,
                               Collection<String> filter) throws IOException {
        this.keyValidator = keyValidator;
        this.isTWCS = isTWCS;
        this.cfStore = cfStore;
        if (snapshotName != null) {
            this.clearSnapshot = false;
        } else {
            snapshotName = Util.generateSnapshotName();
            cfStore.snapshotWithoutMemtable(snapshotName, null, true, null, null, Instant.now());
            this.clearSnapshot = true;
        }
        this.snapshotName = snapshotName;
        this.sstables = cfStore.getSnapshotSSTableReaders(snapshotName);
        if (filter != null) {
            List<org.apache.cassandra.io.sstable.format.SSTableReader> filteredSSTables = new ArrayList<>(sstables.size());
            for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : sstables) {
                String filename = sstable.descriptor.fileFor(SSTableFormat.Components.DATA).name();
                if (filter.contains(filename)) {
                    filteredSSTables.add(sstable);
                }
            }
            this.sstables = filteredSSTables;
        }
    }

    @Override
    public Collection<SSTableReader> getIndexReaders() {
        Collection<SSTableReader> readers = new ArrayList<>(sstables.size());
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : sstables) {
            try {
                Set<Component> discoveredComponents =
                        sstable.descriptor.getComponents(Set.of(), Set.of(BtiFormat.Components.PARTITION_INDEX, BigFormat.Components.PRIMARY_INDEX));

                if (discoveredComponents.isEmpty()) {
                    //Nothing to read.
                    continue;
                }

                if(discoveredComponents.size() > 1){
                    logger.error("Multiple Components found, this should never happen. Filename might be incorrect.");
                }

                Optional<Component> maybeComponent = discoveredComponents.stream().findFirst();

                org.apache.cassandra.io.util.File sstableIndexFile =
                        sstable.descriptor.fileFor(maybeComponent.orElseThrow(() ->
                                new IllegalStateException(String.format("No Component found on sstable %s, this should never happen.", sstable.getFilename()))));

                readers.add(new IndexReader(
                        new SSTableStatistics(
                                sstable.descriptor.id,
                                sstableIndexFile.name(),
                                sstable.uncompressedLength(),
                                sstable.getMinTimestamp(),
                                sstable.getMaxTimestamp(),
                                sstable.getSSTableLevel()),
                        sstable.keyReader(),
                        sstable.getPartitioner()
                ));
            } catch (Throwable t) {
                logger.error("Error opening index readers", t);
            }
        }
        return readers;
    }

    @Override
    public Collection<SSTableReader> getDataReaders() {
        Collection<SSTableReader> readers = new ArrayList<>(sstables.size());
        for (org.apache.cassandra.io.sstable.format.SSTableReader sstable : sstables) {
            try {
                File dataFile = sstable.descriptor.fileFor(SSTableFormat.Components.DATA).toJavaIOFile();
                readers.add(new DataReader(
                        new SSTableStatistics(
                                sstable.descriptor.id,
                                dataFile.getName(),
                                sstable.uncompressedLength(),
                                sstable.getMinTimestamp(),
                                sstable.getMaxTimestamp(),
                                sstable.getSSTableLevel()),
                        sstable.getScanner(),
                        Util.NOW_SECONDS - sstable.metadata().params.gcGraceSeconds
                ));
            } catch (Throwable t) {
                logger.error("Error while getting data readers", t);
            }
        }
        return readers;
    }

    @Override
    public PurgeStatisticsReader getPurgeStatisticsReader() {
        return new PurgeStatisticBackend(cfStore, sstables, cfStore.metadata().params.gcGraceSeconds);
    }

    @Override
    public String formatKey(DecoratedKey key) {
        return keyValidator.getString(key.getKey());
    }

    @Override
    public boolean isTWCS() {
        return isTWCS;
    }

    @Override
    public void close() {
        if (clearSnapshot) {
            cfStore.clearSnapshot(snapshotName);
            clearSnapshot = false;
        }
    }
}
