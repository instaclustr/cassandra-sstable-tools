package com.instaclustr.sstabletools;

import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.commons.cli.*;

import java.util.*;

/**
 * Collect sstable metadata.
 */
public class SSTableMetadataCollector {
    private static final String HELP_OPTION = "h";

    private static final Options options = new Options();
    private static CommandLine cmd;

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ic-sstables <keyspace> <columnFamily>", "Print out metadata for sstables the belong to a column family", options, null);
    }

    public static void main(String[] args) {
        try {
            CommandLineParser parser = new PosixParser();
            try {
                cmd = parser.parse(options, args);
            } catch (ParseException e) {
                System.err.println(e.getMessage());
                printHelp();
                System.exit(1);
            }

            if (cmd.hasOption(HELP_OPTION)) {
                printHelp();
                System.exit(0);
            }

            if (cmd.getArgs().length != 2) {
                printHelp();
                System.exit(1);
            }

            args = cmd.getArgs();
            String ksName = args[0];
            String cfName = args[1];

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
            Class compactionClass = proxy.getCompactionClass(ksName, cfName);
            Comparator<SSTableMetadata> comparator = SSTableMetadata.GENERATION_COMPARATOR;
            if (compactionClass.equals(DateTieredCompactionStrategy.class)) {
                comparator = SSTableMetadata.DTCS_COMPARATOR;
            }
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

            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }
}
