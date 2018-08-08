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

            Histogram sizeHistogram = new Histogram();
            Histogram sstableHistogram = new Histogram();
            long partitionCount = 0;

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
                sizeHistogram.update(stat.size);
                sstableHistogram.update(stat.tableCount);
                partitionCount++;
            }
            sizeHistogram.snapshot();
            sstableHistogram.snapshot();

            cfProxy.close();

            System.out.println("Summary:");
            TableBuilder tb = new TableBuilder();
            tb.setHeader("", "Size", "SSTable");
            tb.addRow("Count", Long.toString(partitionCount), "");
            tb.addRow("Total", Util.humanReadableByteCount(sizeHistogram.getTotal()), Integer.toString(sstableReaders.size()));
            tb.addRow("Minimum", Util.humanReadableByteCount(sizeHistogram.getMin()), Long.toString(sstableHistogram.getMin()));
            tb.addRow("Average", Util.humanReadableByteCount(Math.round(sizeHistogram.getMean())), String.format("%.1f", sstableHistogram.getMean()));
            tb.addRow("std dev.", Util.humanReadableByteCount(Math.round(sizeHistogram.getStdDev())), String.format("%.1f", sstableHistogram.getStdDev()));
            tb.addRow("50%", Util.humanReadableByteCount(Math.round(sizeHistogram.getValue(0.5))), String.format("%.1f", sstableHistogram.getValue(0.5)));
            tb.addRow("75%", Util.humanReadableByteCount(Math.round(sizeHistogram.getValue(0.75))), String.format("%.1f", sstableHistogram.getValue(0.75)));
            tb.addRow("90%", Util.humanReadableByteCount(Math.round(sizeHistogram.getValue(0.9))), String.format("%.1f", sstableHistogram.getValue(0.9)));
            tb.addRow("95%", Util.humanReadableByteCount(Math.round(sizeHistogram.getValue(0.95))), String.format("%.1f", sstableHistogram.getValue(0.95)));
            tb.addRow("99%", Util.humanReadableByteCount(Math.round(sizeHistogram.getValue(0.99))), String.format("%.1f", sstableHistogram.getValue(0.99)));
            tb.addRow("99.9%", Util.humanReadableByteCount(Math.round(sizeHistogram.getValue(0.999))), String.format("%.1f", sstableHistogram.getValue(0.999)));
            tb.addRow("Maximum", Util.humanReadableByteCount(sizeHistogram.getMax()), Long.toString(sstableHistogram.getMax()));
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
                comparator = SSTableStatistics.DTCS_COMPARATOR;
            }
            if (cfProxy.isTWCS()) {
                comparator = SSTableStatistics.TWCS_COMPARATOR;
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
