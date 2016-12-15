package com.instaclustr.sstabletools;

import com.google.common.collect.MinMaxPriorityQueue;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import org.apache.commons.cli.*;

import java.util.*;

/**
 * Collect partition size statistics.
 */
public class PartitionSizeStatisticsCollector {
    private static final String HELP_OPTION = "h";
    private static final String NUMBER_OPTION = "n";
    private static final String SNAPSHOT_OPTION = "t";
    private static final String FILTER_OPTION = "f";
    private static final String BATCH_OPTION = "b";

    private static final Options options = new Options();
    private static CommandLine cmd;

    static {
        Option optHelp = new Option(HELP_OPTION, "help", false, "Print this help message");
        options.addOption(optHelp);

        Option optNumber = new Option(NUMBER_OPTION, true, "Number of partitions to display");
        optNumber.setArgName("num");
        options.addOption(optNumber);

        Option optSnapshot = new Option(SNAPSHOT_OPTION, "snapshot", true, "Snapshot name");
        optSnapshot.setArgName("name");
        options.addOption(optSnapshot);

        Option optFilter = new Option(FILTER_OPTION, "filter", true, "Filter to sstables (comma separated)");
        optFilter.setArgName("files");
        options.addOption(optFilter);

        Option optBatch = new Option(BATCH_OPTION, "batch", false, "Batch mode");
        options.addOption(optBatch);
    }

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ic-pstats <keyspace> <columnFamily>", "Partition size statistics for a column family", options, null);
    }

    public static void main(String[] args) {
        ColumnFamilyProxy cfProxy = null;
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

            final int numPartitions;
            if (cmd.hasOption(NUMBER_OPTION)) {
                numPartitions = Integer.valueOf(cmd.getOptionValue(NUMBER_OPTION));
            } else {
                numPartitions = 10;
            }

            String snapshotName = null;
            if (cmd.hasOption(SNAPSHOT_OPTION)) {
                snapshotName = cmd.getOptionValue(SNAPSHOT_OPTION);
            }

            Collection<String> filter = null;
            if (cmd.hasOption(FILTER_OPTION)) {
                String[] names = cmd.getOptionValue(FILTER_OPTION).split(",");
                filter = Arrays.asList(names);
            }

            boolean interactive = true;
            if (cmd.hasOption(BATCH_OPTION)) {
                interactive = false;
            }

            args = cmd.getArgs();
            String ksName = args[0];
            String cfName = args[1];

            cfProxy = CassandraBackend.getInstance().getColumnFamily(ksName, cfName, snapshotName, filter);
            Collection<SSTableReader> sstableReaders = cfProxy.getIndexReaders();
            long totalLength = 0;
            for (SSTableReader reader : sstableReaders) {
                totalLength += reader.getSSTableStatistics().size;
            }
            if (totalLength == 0) {
                System.out.println("No data found!");
                System.exit(0);
            }

            long minSize = Long.MAX_VALUE;
            long maxSize = 0;
            long partitionCount = 0;
            long totalSize = 0;
            int minTables = Integer.MAX_VALUE;
            int maxTables = 0;
            long totalTables = 0;

            MinMaxPriorityQueue<PartitionStatistics> largestPartitions = MinMaxPriorityQueue
                    .orderedBy(PartitionStatistics.SIZE_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();

            MinMaxPriorityQueue<PartitionStatistics> tableCountLeaders = MinMaxPriorityQueue
                    .orderedBy(PartitionStatistics.SSTABLE_COUNT_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();

            PartitionReader partitionReader = new PartitionReader(sstableReaders, totalLength);
            PartitionStatistics stat;
            ProgressBar progressBar = new ProgressBar("Analyzing SSTables...", interactive);
            progressBar.updateProgress(0.0);
            while ((stat = partitionReader.read()) != null) {
                progressBar.updateProgress(partitionReader.getProgress());
                largestPartitions.add(stat);
                tableCountLeaders.add(stat);
                minSize = Math.min(minSize, stat.size);
                maxSize = Math.max(maxSize, stat.size);
                totalSize += stat.size;
                minTables = Math.min(minTables, stat.tableCount);
                maxTables = Math.max(maxTables, stat.tableCount);
                totalTables += stat.tableCount;
                partitionCount++;
            }

            cfProxy.close();

            System.out.println("Summary:");
            TableBuilder tb = new TableBuilder();
            tb.setHeader("", "Size", "SSTable");
            tb.addRow("Count", Long.toString(partitionCount), "");
            tb.addRow("Total", Util.humanReadableByteCount(totalSize), Integer.toString(sstableReaders.size()));
            tb.addRow("Minimum", Util.humanReadableByteCount(minSize), Integer.toString(minTables));
            tb.addRow("Maximum", Util.humanReadableByteCount(maxSize), Integer.toString(maxTables));
            tb.addRow("Average", Util.humanReadableByteCount(totalSize / partitionCount), String.format("%.1f", totalTables / (double) partitionCount));
            System.out.println(tb);

            System.out.println("Largest partitions:");
            TableBuilder lptb = new TableBuilder();
            lptb.setHeader("Key", "Size", "SSTable Count");

            while (!largestPartitions.isEmpty()) {
                PartitionStatistics p = largestPartitions.remove();
                lptb.addRow(
                        cfProxy.formatKey(p.key),
                        Util.humanReadableByteCount(p.size),
                        Integer.toString(p.tableCount)
                );
            }
            System.out.println(lptb);

            System.out.println("SSTable Leaders:");
            TableBuilder sctb = new TableBuilder();
            sctb.setHeader("Key", "SSTable Count", "Size");
            while (!tableCountLeaders.isEmpty()) {
                PartitionStatistics p = tableCountLeaders.remove();
                sctb.addRow(
                        cfProxy.formatKey(p.key),
                        Long.toString(p.tableCount),
                        Util.humanReadableByteCount(p.size)
                );
            }
            System.out.println(sctb);

            System.out.println("SSTables:");
            TableBuilder cltb = new TableBuilder();
            cltb.setHeader(
                    "SSTable",
                    "Size",
                    "Min Timestamp",
                    "Max Timestamp",
                    "Level",
                    "Partitions",
                    "Avg Partition Size",
                    "Max Partition Size"
            );
            List<SSTableStatistics> sstableStats = partitionReader.getSSTableStatistics();
            Comparator<SSTableStatistics> comparator = SSTableStatistics.LIVENESS_COMPARATOR;
            if (cfProxy.isDTCS()) {
                comparator = SSTableStatistics.TIMESTAMP_COMPARATOR;
            }
            Collections.sort(sstableStats, comparator);
            for (SSTableStatistics stats : sstableStats) {
                cltb.addRow(
                        stats.filename,
                        Util.humanReadableByteCount(stats.size),
                        Util.UTC_DATE_FORMAT.format(new Date(stats.minTimestamp / 1000)),
                        Util.UTC_DATE_FORMAT.format(new Date(stats.maxTimestamp / 1000)),
                        Integer.toString(stats.level),
                        Long.toString(stats.partitionCount),
                        Util.humanReadableByteCount(stats.size / stats.partitionCount),
                        Util.humanReadableByteCount(stats.maxPartitionSize)
                );
            }
            System.out.println(cltb);

            System.exit(0);
        } catch (Throwable t) {
            if (cfProxy != null) {
                cfProxy.close();
            }
            t.printStackTrace();
            System.exit(1);
        }
    }
}
