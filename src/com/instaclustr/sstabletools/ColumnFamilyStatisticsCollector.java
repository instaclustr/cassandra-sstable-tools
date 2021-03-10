package com.instaclustr.sstabletools;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.RateLimiter;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import com.instaclustr.sstabletools.cassandra.CassandraSchema;
import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
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
    private static final String SCHEMA_OPTION = "s";

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

        Option optSchema = new Option(SCHEMA_OPTION, "schema", true, "Load the schema from a YAML definition instead of from disk");
        options.addOption(optSchema);
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

            CassandraSchema schema = null;
            if (cmd.hasOption(SCHEMA_OPTION)) {
                File file = new File(cmd.getOptionValue(SCHEMA_OPTION));
                FileInputStream fileInputStream = new FileInputStream(file);

                schema = new Yaml().loadAs(fileInputStream, CassandraSchema.class);
            }

            args = cmd.getArgs();
            String ksName = args[0];
            String cfName = args[1];

            cfProxy = CassandraBackend.getInstance(schema).getColumnFamily(ksName, cfName, snapshotName, filter);
            Collection<SSTableReader> sstableReaders = cfProxy.getDataReaders(rateLimiter);
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
            Histogram tombstoneHistogram = new Histogram();
            Map<Integer, Long> ttl = new HashMap<>();
            long partitionCount = 0;
            long rowCount = 0;
            long rowDeleteCount = 0;
            long tombstoneCount = 0;

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
                    tombstoneHistogram.update(pStats.tombstoneCount);
                    tombstoneCount += pStats.tombstoneCount;
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
                pStats.mergeTtl(ttl);
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
            tb.addRow("Tombstones", Long.toString(tombstoneCount), "");
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

            if (!ttl.isEmpty()) {
                System.out.println("TTL:");
                TableBuilder ttltb = new TableBuilder();
                ttltb.setHeader("TTL", "Count");
                for (Map.Entry<Integer, Long> entry : ttl.entrySet()) {
                    ttltb.addRow(Util.humanReadableDateDiff(0, entry.getKey() * 1000L), Long.toString(entry.getValue()));
                }
                System.out.println(ttltb);
            }

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
                System.out.println("Tombstone Histogram:");
                Snapshot tombstoneSnapshot = tombstoneHistogram.snapshot();
                TableBuilder tombtb = new TableBuilder();
                tombtb.setHeader("Percentile", "Count");
                tombtb.addRow("Minimum", Long.toString(tombstoneSnapshot.getMin()));
                tombtb.addRow("Average", Long.toString(Math.round(tombstoneSnapshot.getMean())));
                tombtb.addRow("std dev.", Long.toString(Math.round(tombstoneSnapshot.getStdDev())));
                tombtb.addRow("50%", Long.toString(Math.round(tombstoneSnapshot.getPercentile(0.5))));
                tombtb.addRow("75%", Long.toString(Math.round(tombstoneSnapshot.getPercentile(0.75))));
                tombtb.addRow("90%", Long.toString(Math.round(tombstoneSnapshot.getPercentile(0.9))));
                tombtb.addRow("95%", Long.toString(Math.round(tombstoneSnapshot.getPercentile(0.95))));
                tombtb.addRow("99%", Long.toString(Math.round(tombstoneSnapshot.getPercentile(0.99))));
                tombtb.addRow("99.9%", Long.toString(Math.round(tombstoneSnapshot.getPercentile(0.999))));
                tombtb.addRow("Maximum", Long.toString(tombstoneSnapshot.getMax()));
                System.out.println(tombtb);

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
