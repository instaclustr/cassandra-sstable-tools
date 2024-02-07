package com.instaclustr.sstabletools;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.MinMaxPriorityQueue;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import com.instaclustr.sstabletools.cli.CLI;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Collect partition size statistics.
 */
@Command(
    versionProvider = CLI.class,
    name = "purge",
    usageHelpWidth = 128,
    description = "Statistics about reclaimable data for a column family",
    mixinStandardHelpOptions = true
)
public class PurgeStatisticsCollector implements Runnable {

    @Option(names = {"-n"}, description = "Number of partitions to display, defaults to 10", arity = "1", defaultValue = "10")
    public int numPartitions;

    @Option(names = {"-t"}, description = "Snapshot name", arity = "1")
    public String snapshotName;

    @Option(names = {"-f"}, description = "Filter to sstables (comma separated)", defaultValue = "")
    public String filters;

    @Option(names = {"-b"}, description = "Batch mode", arity = "0")
    public boolean batch;

    @Parameters(arity = "2", description = "<keyspace> <table>")
    public List<String> params;

    @Override
    public void run() {

        Collection<String> filter = null;

        if (!filters.isEmpty()) {
            String[] names = filters.split(",");
            filter = Arrays.asList(names);
        }

        boolean interactive = true;
        if (batch) {
            interactive = false;
        }

        final String ksName = params.get(0);
        final String cfName = params.get(1);

        try (ColumnFamilyProxy cfProxy = CassandraBackend.getInstance().getColumnFamily(ksName, cfName, snapshotName, filter)) {
            PurgeStatisticsReader reader = cfProxy.getPurgeStatisticsReader();

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
                    stats.ssTableIds.toString()
                );
            }
            System.out.println(tb);
        }
    }
}
