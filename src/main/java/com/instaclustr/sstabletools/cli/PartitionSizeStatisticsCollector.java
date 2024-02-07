package com.instaclustr.sstabletools.cli;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.google.common.collect.MinMaxPriorityQueue;
import com.instaclustr.sstabletools.ColumnFamilyProxy;
import com.instaclustr.sstabletools.Histogram;
import com.instaclustr.sstabletools.PartitionReader;
import com.instaclustr.sstabletools.PartitionStatistics;
import com.instaclustr.sstabletools.ProgressBar;
import com.instaclustr.sstabletools.SSTableReader;
import com.instaclustr.sstabletools.SSTableStatistics;
import com.instaclustr.sstabletools.Snapshot;
import com.instaclustr.sstabletools.TableBuilder;
import com.instaclustr.sstabletools.Util;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
    versionProvider = CLI.class,
    name = "pstats",
    usageHelpWidth = 128,
    description = "Partition size statistics for a column family",
    mixinStandardHelpOptions = true
)
public class PartitionSizeStatisticsCollector implements Runnable {

    @Option(names = {"-n"}, description = "Number of partitions to display", arity = "1", defaultValue = "10")
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

        try (final ColumnFamilyProxy cfProxy = CassandraBackend.getInstance().getColumnFamily(ksName, cfName, snapshotName, filter)) {
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
            Snapshot sizeSnapshot = sizeHistogram.snapshot();
            Snapshot sstableSnapshot = sstableHistogram.snapshot();

            cfProxy.close();

            System.out.println("Summary:");
            TableBuilder tb = new TableBuilder();
            tb.setHeader("", "Size", "SSTable");
            tb.addRow("Count", Long.toString(partitionCount), "");
            tb.addRow("Total", Util.humanReadableByteCount(sizeSnapshot.getTotal()), Integer.toString(sstableReaders.size()));
            tb.addRow("Minimum", Util.humanReadableByteCount(sizeSnapshot.getMin()), Long.toString(sstableSnapshot.getMin()));
            tb.addRow("Average", Util.humanReadableByteCount(Math.round(sizeSnapshot.getMean())), String.format("%.1f", sstableSnapshot.getMean()));
            tb.addRow("std dev.", Util.humanReadableByteCount(Math.round(sizeSnapshot.getStdDev())), String.format("%.1f", sstableSnapshot.getStdDev()));
            tb.addRow("50%", Util.humanReadableByteCount(Math.round(sizeSnapshot.getPercentile(0.5))), String.format("%.1f", sstableSnapshot.getPercentile(0.5)));
            tb.addRow("75%", Util.humanReadableByteCount(Math.round(sizeSnapshot.getPercentile(0.75))), String.format("%.1f", sstableSnapshot.getPercentile(0.75)));
            tb.addRow("90%", Util.humanReadableByteCount(Math.round(sizeSnapshot.getPercentile(0.9))), String.format("%.1f", sstableSnapshot.getPercentile(0.9)));
            tb.addRow("95%", Util.humanReadableByteCount(Math.round(sizeSnapshot.getPercentile(0.95))), String.format("%.1f", sstableSnapshot.getPercentile(0.95)));
            tb.addRow("99%", Util.humanReadableByteCount(Math.round(sizeSnapshot.getPercentile(0.99))), String.format("%.1f", sstableSnapshot.getPercentile(0.99)));
            tb.addRow("99.9%", Util.humanReadableByteCount(Math.round(sizeSnapshot.getPercentile(0.999))), String.format("%.1f", sstableSnapshot.getPercentile(0.999)));
            tb.addRow("Maximum", Util.humanReadableByteCount(sizeSnapshot.getMax()), Long.toString(sstableSnapshot.getMax()));
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
        }
    }
}
