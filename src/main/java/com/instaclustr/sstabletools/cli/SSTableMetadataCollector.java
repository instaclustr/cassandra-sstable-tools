package com.instaclustr.sstabletools.cli;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.instaclustr.sstabletools.CassandraProxy;
import com.instaclustr.sstabletools.SSTableMetadata;
import com.instaclustr.sstabletools.TableBuilder;
import com.instaclustr.sstabletools.Util;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * Collect sstable metadata.
 */
@Command(
    versionProvider = CLI.class,
    name = "sstables",
    usageHelpWidth = 128,
    description = "Print out metadata for sstables that belong to a column family",
    mixinStandardHelpOptions = true
)
public class SSTableMetadataCollector implements Runnable {

    @Parameters(arity = "2", description = "<keyspace> <table>")
    public List<String> params;

    @Override
    public void run() {
        String ksName = params.get(0);
        String cfName = params.get(1);

        TableBuilder tb = new TableBuilder();
        tb.setHeader(
            "SSTable",
            "Disk Size",
            "Total Size",
            "Min Timestamp",
            "Max Timestamp",
            "File Timestamp",
            "Duration",
            "Min Deletion Time",
            "Max Deletion Time",
            "Level",
            "Keys",
            "Avg Partition Size",
            "Max Partition Size",
            "Avg Column Count",
            "Max Column Count",
            "Droppable",
            "Repaired At"
        );
        CassandraProxy proxy = CassandraBackend.getInstance();
        List<SSTableMetadata> metadataCollection = proxy.getSSTableMetadata(ksName, cfName);
        Class<?> compactionClass = proxy.getCompactionClass(ksName, cfName);
        Comparator<SSTableMetadata> comparator = SSTableMetadata.GENERATION_COMPARATOR;
        if (compactionClass.equals(TimeWindowCompactionStrategy.class)) {
            comparator = SSTableMetadata.TWCS_COMPARATOR;
        }
        if (compactionClass.equals(LeveledCompactionStrategy.class)) {
            comparator = SSTableMetadata.LEVEL_COMPARATOR;
        }

        Collections.sort(metadataCollection, comparator);
        for (SSTableMetadata metadata : metadataCollection) {
            tb.addRow(
                metadata.filename,
                Util.humanReadableByteCount(metadata.diskLength),
                Util.humanReadableByteCount(metadata.uncompressedLength),
                Util.UTC_DATE_FORMAT.format(new Date(metadata.minTimestamp / 1000)),
                Util.UTC_DATE_FORMAT.format(new Date(metadata.maxTimestamp / 1000)),
                Util.UTC_DATE_FORMAT.format(new Date(metadata.fileTimestamp)),
                Util.humanReadableDateDiff(metadata.minTimestamp / 1000, metadata.maxTimestamp / 1000),
                metadata.minLocalDeletionTime != Integer.MAX_VALUE ? Util.UTC_DATE_FORMAT.format(new Date(metadata.minLocalDeletionTime * 1000L)) : "",
                metadata.maxLocalDeletionTime != Integer.MAX_VALUE ? Util.UTC_DATE_FORMAT.format(new Date(metadata.maxLocalDeletionTime * 1000L)) : "",
                Integer.toString(metadata.level),
                Long.toString(metadata.keys),
                Util.humanReadableByteCount(metadata.avgRowSize),
                Util.humanReadableByteCount(metadata.maxRowSize),
                Long.toString(metadata.avgColumnCount),
                Long.toString(metadata.maxColumnCount),
                Double.toString(metadata.droppableTombstones),
                metadata.isRepaired ? Util.UTC_DATE_FORMAT.format(new Date(metadata.repairedAt)) : ""
            );
        }
        System.out.println(tb);
    }
}
