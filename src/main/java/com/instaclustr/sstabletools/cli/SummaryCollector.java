package com.instaclustr.sstabletools.cli;

import java.util.Date;
import java.util.List;

import com.instaclustr.sstabletools.CassandraProxy;
import com.instaclustr.sstabletools.SSTableMetadata;
import com.instaclustr.sstabletools.TableBuilder;
import com.instaclustr.sstabletools.Util;
import com.instaclustr.sstabletools.cassandra.CassandraBackend;
import picocli.CommandLine.Command;

/**
 * Display summary about column families.
 */
@Command(
    versionProvider = CLI.class,
    name = "summary",
    usageHelpWidth = 128,
    description = "Summary information about all column families including how much of the data is repaired",
    mixinStandardHelpOptions = true
)
public class SummaryCollector implements Runnable {

    @Override
    public void run() {

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
                long repaired = 0;
                long repairedLength = 0;
                for (SSTableMetadata metadata : metadataCollection) {
                    diskSize += metadata.diskLength;
                    dataSize += metadata.uncompressedLength;
                    if (metadata.isRepaired) {
                        repaired++;
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
                    repaired > 0 ? Util.UTC_DATE_FORMAT.format(new Date(repairedAt)) : "",
                    repaired > 0 ? String.format("%d/%d %d%%", repaired, metadataCollection.size(), (int) Math.floor((repairedLength / (double) dataSize) * 100)) : ""
                );
            }
        }

        System.out.println(tb);
    }
}
