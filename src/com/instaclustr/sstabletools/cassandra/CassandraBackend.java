package com.instaclustr.sstabletools.cassandra;

import com.google.common.collect.Maps;
import com.instaclustr.sstabletools.CassandraProxy;
import com.instaclustr.sstabletools.ColumnFamilyProxy;
import com.instaclustr.sstabletools.SSTableMetadata;
import com.instaclustr.sstabletools.Util;
import com.instaclustr.sstabletools.cassandra.CassandraSchema.ColumnFamily;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

/**
 * Proxy to Cassandra 2.2 backend.
 */
public class CassandraBackend implements CassandraProxy {
    private static final Logger logger = LoggerFactory.getLogger(CassandraBackend.class);

    private static CassandraBackend singleton = null;

    public static CassandraProxy getInstance(CassandraSchema schema) {
        if (singleton == null) {
            singleton = new CassandraBackend(schema);
        }
        return singleton;
    }

    static {
        org.apache.cassandra.tools.Util.initDatabaseDescriptor();
    }

    private CassandraBackend(CassandraSchema schema) {
        if (schema == null) {
            Schema.instance.loadFromDisk(false);
        } else {
            for (CassandraSchema.Keyspace keyspace : schema.keyspaces) {
                List<CFMetaData> cfDefs = new ArrayList<>();

                for (ColumnFamily columnFamily : keyspace.columnFamilies) {
                    CFMetaData cfMetaData = CFMetaData.compile(columnFamily.cql, keyspace.name);
                    cfDefs.add(cfMetaData);

                    logger.warn("Loading column family {} with id {}", cfMetaData.cfName, cfMetaData.cfId.toString().replace("-", ""));
                }

                HashMap<String, String> options = Maps.newHashMap();
                options.put("replication_factor", "1");

                logger.warn("Adding {} column families to keyspace {}", cfDefs.size(), keyspace.name);

                KSMetaData ksMetaData = KSMetaData.newKeyspace(keyspace.name, SimpleStrategy.class, options, false, cfDefs);

                Schema.instance.addKeyspace(ksMetaData);
            }
        }
    }

    public List<String> getKeyspaces() {
        List<String> names = new ArrayList<>(Schema.instance.getNonSystemKeyspaces());
        Collections.sort(names);
        return names;
    }

    public List<String> getColumnFamilies(String ksName) {
        Map<String,CFMetaData> cfMetaDataMap = Schema.instance.getKSMetaData(ksName).cfMetaData();
        List<String> names = new ArrayList<>(cfMetaDataMap.keySet());
        Collections.sort(names);
        return names;
    }

    private ColumnFamilyStore getStore(String ksName, String cfName) {
        // Start by validating keyspace name
        if (Schema.instance.getKSMetaData(ksName) == null) {
            System.err.println(String.format("Reference to nonexistent keyspace: %s!", ksName));
            System.exit(1);
        }
        Keyspace keyspace = Keyspace.open(ksName);

        // Make it works for indexes too - find parent cf if necessary
        String baseName = cfName;
        if (cfName.contains(".")) {
            String[] parts = cfName.split("\\.", 2);
            baseName = parts[0];
        }

        // IllegalArgumentException will be thrown here if ks/cf pair does not exist
        try {
            return keyspace.getColumnFamilyStore(baseName);
        } catch (Throwable t) {
            System.err.println(String.format(
                    "The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
                    ksName, cfName));
            System.exit(1);
        }
        return null;
    }

    public List<SSTableMetadata> getSSTableMetadata(String ksName, String cfName) {
        ColumnFamilyStore cfStore = getStore(ksName, cfName);
        Collection<SSTableReader> tables = cfStore.getSSTables();
        List<SSTableMetadata> metaData = new ArrayList<>(tables.size());
        for (SSTableReader table : tables) {
            SSTableMetadata tableMetadata = new SSTableMetadata();
            File dataFile = new File(table.descriptor.filenameFor(Component.DATA));
            tableMetadata.filename = dataFile.getName();
            tableMetadata.generation = table.descriptor.generation;
            try {
                tableMetadata.fileTimestamp = Files.getLastModifiedTime(dataFile.toPath()).toMillis();
            } catch (IOException e) {
                tableMetadata.fileTimestamp = 0;
            }
            tableMetadata.minTimestamp = table.getMinTimestamp();
            tableMetadata.maxTimestamp = table.getMaxTimestamp();
            tableMetadata.maxLocalDeletionTime = table.getSSTableMetadata().maxLocalDeletionTime;
            tableMetadata.diskLength = table.onDiskLength();
            tableMetadata.uncompressedLength = table.uncompressedLength();
            tableMetadata.keys = table.estimatedKeys();
            EstimatedHistogram rowSizeHistogram = table.getEstimatedRowSize();
            tableMetadata.maxRowSize = rowSizeHistogram.max();
            tableMetadata.avgRowSize = rowSizeHistogram.mean();
            EstimatedHistogram columnCountHistogram = table.getEstimatedColumnCount();
            tableMetadata.maxColumnCount = columnCountHistogram.max();
            tableMetadata.avgColumnCount = columnCountHistogram.mean();
            tableMetadata.droppableTombstones = table.getDroppableTombstonesBefore(Util.NOW_SECONDS - table.metadata.getGcGraceSeconds());
            tableMetadata.level = table.getSSTableLevel();
            tableMetadata.isRepaired = table.isRepaired();
            tableMetadata.repairedAt = table.getSSTableMetadata().repairedAt;
            metaData.add(tableMetadata);
        }
        return metaData;
    }

    public ColumnFamilyProxy getColumnFamily(String ksName, String cfName, String snapshotName, Collection<String> filter) {
        ColumnFamilyStore cfStore = getStore(ksName, cfName);
        try {
            CFMetaData metaData = Schema.instance.getCFMetaData(ksName, cfName);
            return new ColumnFamilyBackend(
                metaData.getKeyValidator(),
                cfStore.getCompactionStrategyClass().equals("org.apache.cassandra.db.compaction.DateTieredCompactionStrategy"),
                cfStore,
                snapshotName,
                filter);
        } catch (Throwable t) {
            System.err.println(String.format("Error retrieving snapshot for %s.%s", ksName, cfName));
            System.exit(1);
        }
        return null;
    }
}
