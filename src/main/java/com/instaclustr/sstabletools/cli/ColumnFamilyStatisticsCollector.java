package com.instaclustr.sstabletools.cli;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.RateLimiter;
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
    name = "cfstats",
    usageHelpWidth = 128,
    description = "Detailed statistics about cells in a column family",
    mixinStandardHelpOptions = true
)
public class ColumnFamilyStatisticsCollector implements Runnable {

    @Option(names = {"-n"}, description = "Number of partitions to display, defaults to 10", arity = "1", defaultValue = "10")
    public int numPartitions;

    @Option(names = {"-t"}, description = "Snapshot name", arity = "1")
    public String snapshotName;

    @Option(names = {"-f"}, description = "Filter to sstables (comma separated", defaultValue = "")
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
            Collection<SSTableReader> sstableReaders = cfProxy.getDataReaders();
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
            Histogram rowHistogram = new Histogram();
            long partitionCount = 0;
            long rowCount = 0;
            long rowDeleteCount = 0;

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
                widestPartitions.add(pStats);
                largestPartitions.add(pStats);
                if (pStats.tombstoneCount > 0) {
                    tombstoneLeaders.add(pStats);
                }
                if (pStats.rowDeleteCount > 0) {
                    mostDeletedRows.add(pStats);
                }
                tableCountLeaders.add(pStats);
                sizeHistogram.update(pStats.size);
                sstableHistogram.update(pStats.tableCount);
                rowHistogram.update(pStats.rowCount);
                rowCount += pStats.rowCount;
                rowDeleteCount += pStats.rowDeleteCount;
                partitionCount++;
            }
            Snapshot sizeSnapshot = sizeHistogram.snapshot();
            Snapshot sstableSnapshot = sstableHistogram.snapshot();
            Snapshot rowSnapshot = rowHistogram.snapshot();

            cfProxy.close();

            System.out.println("Summary:");
            TableBuilder tb = new TableBuilder();
            tb.setHeader("", "Size", "SSTable");
            tb.addRow("Count", Long.toString(partitionCount), "");
            tb.addRow("Rows", Long.toString(rowCount), "");
            tb.addRow("(deleted)", Long.toString(rowDeleteCount), "");
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

            System.out.println("Row Histogram:");
            TableBuilder rhtb = new TableBuilder();
            rhtb.setHeader("Percentile", "Count");
            rhtb.addRow("Minimum", Long.toString(rowSnapshot.getMin()));
            rhtb.addRow("Average", Long.toString(Math.round(rowSnapshot.getMean())));
            rhtb.addRow("std dev.", Long.toString(Math.round(rowSnapshot.getStdDev())));
            rhtb.addRow("50%", Long.toString(Math.round(rowSnapshot.getPercentile(0.5))));
            rhtb.addRow("75%", Long.toString(Math.round(rowSnapshot.getPercentile(0.75))));
            rhtb.addRow("90%", Long.toString(Math.round(rowSnapshot.getPercentile(0.9))));
            rhtb.addRow("95%", Long.toString(Math.round(rowSnapshot.getPercentile(0.95))));
            rhtb.addRow("99%", Long.toString(Math.round(rowSnapshot.getPercentile(0.99))));
            rhtb.addRow("99.9%", Long.toString(Math.round(rowSnapshot.getPercentile(0.999))));
            rhtb.addRow("Maximum", Long.toString(rowSnapshot.getMax()));
            System.out.println(rhtb);

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
            System.out.println(sctb);

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
                    ts.getLiveness() + "%"
                );
            }
            System.out.println(cltb);
        }
    }
}
