package com.instaclustr.sstabletools;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.RateLimiter;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import org.apache.commons.cli.*;

import java.util.*;

/**
 * Collect statistics about a column family.
 */
public class ColumnFamilyStatisticsCollector {
    private static final String HELP_OPTION = "h";
    private static final String NUMBER_OPTION = "n";
    private static final String RATELIMIT_OPTION = "r";
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

        Option optRateLimit = new Option(RATELIMIT_OPTION, "rate", true, "Limit read throughput (in Mb/s)");
        optRateLimit.setArgName("limit");
        options.addOption(optRateLimit);

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
        formatter.printHelp("ic-cfstats <keyspace> <columnFamily>", "Detailed statistics about cells in a column family", options, null);
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

            RateLimiter rateLimiter = null;
            if (cmd.hasOption(RATELIMIT_OPTION)) {
                double bytesPerSecond = Integer.valueOf(cmd.getOptionValue(RATELIMIT_OPTION)) * 1024.0 * 1024.0;
                rateLimiter = RateLimiter.create(bytesPerSecond);
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
            Collection<SSTableReader> sstableReaders = cfProxy.getDataReaders(rateLimiter);
            long totalLength = 0;
            for (SSTableReader reader : sstableReaders) {
                totalLength += reader.getSSTableStatistics().size;
            }
            if (totalLength == 0) {
                System.out.println("No data found!");
                System.exit(0);
            }

            long minPartitionSize = Long.MAX_VALUE;
            long maxPartitionSize = 0;
            long partitionCount = 0;
            long rowCount = 0;
            long totalPartitionSize = 0;
            int minTables = Integer.MAX_VALUE;
            int maxTables = 0;
            long totalTables = 0;

            MinMaxPriorityQueue<PartitionStatistics> largestPartitions = MinMaxPriorityQueue
                    .orderedBy(PartitionStatistics.SIZE_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();

            MinMaxPriorityQueue<PartitionStatistics> widestPartitions = MinMaxPriorityQueue
                    .orderedBy(PartitionStatistics.WIDE_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();

            MinMaxPriorityQueue<PartitionStatistics> tombstoneLeaders = MinMaxPriorityQueue
                    .orderedBy(PartitionStatistics.TOMBSTONE_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();

            MinMaxPriorityQueue<PartitionStatistics> mostDeletedRows = MinMaxPriorityQueue
                    .orderedBy(PartitionStatistics.MOST_DELETED_ROWS_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();

            MinMaxPriorityQueue<PartitionStatistics> tableCountLeaders = MinMaxPriorityQueue
                    .orderedBy(PartitionStatistics.SSTABLE_COUNT_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();

            PartitionReader partitionReader = new PartitionReader(sstableReaders, totalLength);
            PartitionStatistics pStats;
            ProgressBar progressBar = new ProgressBar("Analyzing SSTables...", interactive);
            progressBar.updateProgress(0.0);
            while ((pStats = partitionReader.read()) != null) {
                progressBar.updateProgress(partitionReader.getProgress());
                long partitionSize = pStats.size;
                widestPartitions.add(pStats);
                largestPartitions.add(pStats);
                if (pStats.tombstoneCount > 0) {
                    tombstoneLeaders.add(pStats);
                }
                if (pStats.rowDeleteCount > 0) {
                    mostDeletedRows.add(pStats);
                }
                tableCountLeaders.add(pStats);
                minPartitionSize = Math.min(minPartitionSize, partitionSize);
                maxPartitionSize = Math.max(maxPartitionSize, partitionSize);
                totalPartitionSize += partitionSize;
                minTables = Math.min(minTables, pStats.tableCount);
                maxTables = Math.max(maxTables, pStats.tableCount);
                totalTables += pStats.tableCount;
                rowCount += pStats.rowCount;
                partitionCount++;
            }

            cfProxy.close();

            System.out.println("Summary:");
            TableBuilder tb = new TableBuilder();
            tb.setHeader("", "Size", "SSTable");
            tb.addRow("Count", Long.toString(partitionCount), "");
            tb.addRow("Rows", Long.toString(rowCount), "");
            tb.addRow("Total", Util.humanReadableByteCount(totalPartitionSize), Integer.toString(sstableReaders.size()));
            tb.addRow("Minimum", Util.humanReadableByteCount(minPartitionSize), Integer.toString(minTables));
            tb.addRow("Maximum", Util.humanReadableByteCount(maxPartitionSize), Integer.toString(maxTables));
            tb.addRow("Average", Util.humanReadableByteCount(totalPartitionSize / partitionCount), String.format("%.1f", totalTables / (double) partitionCount));
            System.out.println(tb);

            System.out.println("Largest partitions:");
            TableBuilder lptb = new TableBuilder();
            lptb.setHeader("Key", "Size", "Rows", "(deleted)", "Tombstones", "(droppable)", "Cells", "SSTable Count");

            while (!largestPartitions.isEmpty()) {
                PartitionStatistics p = largestPartitions.remove();
                lptb.addRow(
                        cfProxy.formatKey(p.key),
                        Util.humanReadableByteCount(p.size),
                        Long.toString(p.rowCount),
                        Long.toString(p.rowDeleteCount),
                        Long.toString(p.tombstoneCount),
                        Long.toString(p.droppableTombstoneCount),
                        Long.toString(p.cellCount),
                        Long.toString(p.tableCount)
                );
            }
            System.out.println(lptb);

            System.out.println("Widest partitions:");
            TableBuilder wptb = new TableBuilder();
            wptb.setHeader("Key", "Rows", "(deleted)", "Cells", "Tombstones", "(droppable)", "Size", "SSTable Count");
            while (!widestPartitions.isEmpty()) {
                PartitionStatistics p = widestPartitions.remove();
                wptb.addRow(
                        cfProxy.formatKey(p.key),
                        Long.toString(p.rowCount),
                        Long.toString(p.rowDeleteCount),
                        Long.toString(p.cellCount),
                        Long.toString(p.tombstoneCount),
                        Long.toString(p.droppableTombstoneCount),
                        Util.humanReadableByteCount(p.size),
                        Long.toString(p.tableCount)
                );
            }
            System.out.println(wptb);

            if (!mostDeletedRows.isEmpty()) {
                System.out.println("Most Deleted Rows:");
                TableBuilder mdtb = new TableBuilder();
                mdtb.setHeader("Key", "Rows", "(deleted)", "Size", "SSTable Count");
                while (!mostDeletedRows.isEmpty()) {
                    PartitionStatistics p = mostDeletedRows.remove();
                    mdtb.addRow(
                            cfProxy.formatKey(p.key),
                            Long.toString(p.rowCount),
                            Long.toString(p.rowDeleteCount),
                            Util.humanReadableByteCount(p.size),
                            Long.toString(p.tableCount)
                    );
                }
                System.out.println(mdtb);
            }

            if (!tombstoneLeaders.isEmpty()) {
                System.out.println("Tombstone Leaders:");
                TableBuilder tltb = new TableBuilder();
                tltb.setHeader("Key", "Tombstones", "(droppable)", "Rows", "Cells", "Size", "SSTable Count");
                while (!tombstoneLeaders.isEmpty()) {
                    PartitionStatistics p = tombstoneLeaders.remove();
                    tltb.addRow(
                            cfProxy.formatKey(p.key),
                            Long.toString(p.tombstoneCount),
                            Long.toString(p.droppableTombstoneCount),
                            Long.toString(p.rowCount),
                            Long.toString(p.cellCount),
                            Util.humanReadableByteCount(p.size),
                            Long.toString(p.tableCount)
                    );
                }
                System.out.println(tltb);
            }

            System.out.println("SSTable Leaders:");
            TableBuilder sctb = new TableBuilder();
            sctb.setHeader("Key", "SSTable Count", "Size", "Rows", "Cells", "Tombstones", "(droppable)");
            while (!tableCountLeaders.isEmpty()) {
                PartitionStatistics p = tableCountLeaders.remove();
                sctb.addRow(
                        cfProxy.formatKey(p.key),
                        Long.toString(p.tableCount),
                        Util.humanReadableByteCount(p.size),
                        Long.toString(p.rowCount),
                        Long.toString(p.cellCount),
                        Long.toString(p.tombstoneCount),
                        Long.toString(p.droppableTombstoneCount)
                );
            }
            System.out.print(sctb);

            System.out.println("SSTables:");
            TableBuilder cltb = new TableBuilder();
            cltb.setHeader(
                    "SSTable",
                    "Size",
                    "Min Timestamp",
                    "Max Timestamp",
                    "Partitions",
                    "(deleted)",
                    "(avg size)",
                    "(max size)",
                    "Rows",
                    "(deleted)",
                    "Cells",
                    "(expiring)",
                    "Tombstones",
                    "(droppable)",
                    "(range)",
                    "Cell Liveness"
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
            for (SSTableStatistics ts : sstableStats) {
                cltb.addRow(
                        ts.filename,
                        Util.humanReadableByteCount(ts.size),
                        Util.UTC_DATE_FORMAT.format(new Date(ts.minTimestamp / 1000)),
                        Util.UTC_DATE_FORMAT.format(new Date(ts.maxTimestamp / 1000)),
                        Long.toString(ts.partitionCount),
                        Long.toString(ts.partitionDeleteCount),
                        Util.humanReadableByteCount(ts.size / ts.partitionCount),
                        Util.humanReadableByteCount(ts.maxPartitionSize),
                        Long.toString(ts.rowCount),
                        Long.toString(ts.rowDeleteCount),
                        Long.toString(ts.cellCount),
                        Long.toString(ts.expiringCellCount),
                        Long.toString(ts.tombstoneCount),
                        Long.toString(ts.droppableTombstoneCount),
                        Long.toString(ts.rangeTombstoneCount),
                        Integer.toString(ts.getLiveness()) + "%"
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
