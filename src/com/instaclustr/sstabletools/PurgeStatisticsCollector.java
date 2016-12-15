package com.instaclustr.sstabletools;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.RateLimiter;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import org.apache.commons.cli.*;

import java.util.*;

/**
 * Collect partition size statistics.
 */
public class PurgeStatisticsCollector {
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
        formatter.printHelp("ic-purge <keyspace> <columnFamily>", "Statistics about reclaimable data for a column family", options, null);
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
            PurgeStatisticsReader reader = cfProxy.getPurgeStatisticsReader(rateLimiter);

            long totalSize = 0;
            long totalReclaim = 0;

            MinMaxPriorityQueue<PurgeStatistics> largestPartitions = MinMaxPriorityQueue
                    .orderedBy(PurgeStatistics.PURGE_COMPARATOR)
                    .maximumSize(numPartitions)
                    .create();
            ProgressBar progressBar = new ProgressBar("Analyzing SSTables...", interactive);
            progressBar.updateProgress(0.0);
            while (reader.hasNext()) {
                PurgeStatistics stats = reader.next();
                largestPartitions.add(stats);
                totalSize += stats.size;
                totalReclaim += stats.reclaimable;
                progressBar.updateProgress(reader.getProgress());
            }

            cfProxy.close();

            System.out.println("Summary:");
            TableBuilder tb = new TableBuilder();
            tb.setHeader("", "Size");
            tb.addRow("Disk", Util.humanReadableByteCount(totalSize));
            tb.addRow("Reclaim", Util.humanReadableByteCount(totalReclaim));
            System.out.println(tb);

            System.out.println("Largest reclaimable partitions:");
            tb = new TableBuilder();
            tb.setHeader("Key", "Size", "Reclaim", "Generations");
            while (!largestPartitions.isEmpty()) {
                PurgeStatistics stats = largestPartitions.remove();
                tb.addRow(
                        cfProxy.formatKey(stats.key),
                        Util.humanReadableByteCount(stats.size),
                        Util.humanReadableByteCount(stats.reclaimable),
                        stats.generations.toString()
                );
            }
            System.out.println(tb);

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
