package com.instaclustr.sstabletools.cassandra;

import java.util.List;

public class CassandraSchema {
    public List<Keyspace> keyspaces;

    public CassandraSchema() {
    }

    public static class Keyspace {
        public String name;
        public List<ColumnFamily> columnFamilies;

        public Keyspace() {
        }
    }

    public static class ColumnFamily {
        public String cql;

        public ColumnFamily(String cql) {
            this.cql = cql;
        }
    }
}
