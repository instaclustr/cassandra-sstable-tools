package com.instaclustr.sstabletools;

import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import org.apache.commons.cli.*;

import java.util.Date;
import java.util.List;

/**
 * Display summary about column families.
 */
public class SummaryCollector {
    private static final String HELP_OPTION = "h";

    private static final Options options = new Options();
    private static CommandLine cmd;

    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ic-summary", "Summary information about all column families including how much of the data is repaired", options, null);
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

            TableBuilder tb = new TableBuilder();
            tb.setHeader(
                    "Keyspace",
                    "Column Family",
                    "SSTables",
                    "Disk Size",
                    "Data Size",
                    "Last Repaired",
                    "Repair %"
            );

            CassandraProxy backend = CassandraBackend.getInstance();
            for (String ksName : backend.getKeyspaces()) {
                for (String cfName : backend.getColumnFamilies(ksName)) {
                    List<SSTableMetadata> metadataCollection = CassandraBackend.getInstance().getSSTableMetadata(ksName, cfName);
                    long diskSize = 0;
                    long dataSize = 0;
                    long repairedAt = Long.MIN_VALUE;
                    boolean isRepaired = false;
                    long repairedLength = 0;
                    for (SSTableMetadata metadata : metadataCollection) {
                        diskSize += metadata.diskLength;
                        dataSize += metadata.uncompressedLength;
                        if (metadata.isRepaired) {
                            isRepaired = true;
                            repairedAt = Math.max(repairedAt, metadata.repairedAt);
                            repairedLength += metadata.uncompressedLength;
                        }
                    }
                    tb.addRow(
                            ksName,
                            cfName,
                            Integer.toString(metadataCollection.size()),
                            Util.humanReadableByteCount(diskSize),
                            Util.humanReadableByteCount(dataSize),
                            isRepaired ? Util.UTC_DATE_FORMAT.format(new Date(repairedAt)) : "",
                            isRepaired ? String.format("%d%%", Math.round((repairedLength / (double) dataSize) * 100)) : ""
                    );
                }
            }

            System.out.println(tb);

            System.exit(0);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

}
