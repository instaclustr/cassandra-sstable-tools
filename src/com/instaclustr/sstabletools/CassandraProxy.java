package com.instaclustr.sstabletools;

import java.util.Collection;
import java.util.List;

/**
 * Proxy to Cassandra backend.
 */
public interface CassandraProxy {
    /**
     * Get list of keyspaces.
     *
     * @return Keyspace names
     */
    public List<String> getKeyspaces();

    /**
     *
     * @param ksName Keyspace name.
     * @return Column family names
     */
    public List<String> getColumnFamilies(String ksName);

    /**
     * Get metadata about sstables.
     *
     * @param ksName Keyspace name.
     * @param cfName Column family name.
     * @return SSTable metadata.
     */
    public List<SSTableMetadata> getSSTableMetadata(String ksName, String cfName);

    /**
     * Get proxy to cassandra column family backend.
     *
     * @param ksName Keyspace name.
     * @param cfName Column family name.
     * @param snapshotName Snapshot name to use, or null to one generated.
     * @param filter List of SSTables to analyse, or null to include all.
     * @return Proxy to cassandra column family backend.
     */
    public ColumnFamilyProxy getColumnFamily(String ksName, String cfName, String snapshotName, Collection<String> filter);
}
