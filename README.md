# Compile
```
$ git clone git@github.com:instaclustr/cassandra-sstable-tools.git
$ cd cassandra-sstable-tools
# Select the correct branch for major version (default is cassandra-3.11)
$ git checkout cassandra-3.11
$ ant
```

You can compile against an older minor version with `-Dcassandra.version=<version>`. For example: 

```
$ ant -Dcassandra.version=3.11.2
```

However only the version specified in build.xml is officially supported,
as compatibility between minor versions can break.

# Install
Copy ic-sstable-tools.jar to Cassandra JAR folder, eg. `/usr/share/cassandra`

Copy the bin/ic-* files into your $PATH

# Documentation

| Command     | Description                                                                              |
|-------------|------------------------------------------------------------------------------------------|
| ic-summary  | Summary information about all column families including how much of the data is repaired |
| ic-sstables | Print out metadata for sstables the belong to a column family                            |
| ic-pstats   | Partition size statistics for a column family                                            |
| ic-cfstats  | Detailed statistics about cells in a column family                                       |
| ic-purge    | Statistics about reclaimable data for a column family                                    |

## ic-summary ##
Provides summary information about all column families. Useful for finding
the largest column families and how much data has been repaired by incremental repairs.

### Usage ###

    ic-summary

### Output ###
| Column        | Description                                                 |
|---------------|-------------------------------------------------------------|
| Keyspace      | Keyspace the column family belongs to                       |
| Column Family | Name of column family                                       |
| SSTables      | Number of sstables on this node for the column family       |
| Disk Size     | Compressed size on disk for this node                       |
| Data Size     | Uncompressed size of the data for this node                 |
| Last Repaired | Maximum repair timestamp on sstables                        |
| Repair %      | Percentage of data marked as repaired                       |

## ic-sstables ##
Print out sstable metadata for a column family. Useful in helping to tune compaction settings.

### Usage ###

    ic-sstables <keyspace> <column-family>

### Output ###
| Column             | Description                                               |
|--------------------|-----------------------------------------------------------|
| SSTable            | Data.db filename of sstable                               |
| Disk Size          | Size of sstable on disk                                   |
| Total Size         | Uncompressed size of data contained in the sstable        |
| Min Timestamp      | Minimum cell timestamp contained in the sstable           |
| Max Timestamp      | Maximum cell timestamp contained in the sstable           |
| Duration           | The time span between minimum and maximum cell timestamps |
| Min Deletion Time  | The minimum deletion time                                 |
| Max Deletion Time  | The maximum deletion time                                 |
| Level              | Leveled Tiered Compaction sstable level                   |
| Keys               | Number of partition keys                                  |
| Avg Partition Size | Average partition size                                    |
| Max Partition Size | Maximum partition size                                    |
| Avg Column Count   | Average number of columns in a partition                  |
| Max Column Count   | Maximum number of columns in a partition                  |
| Droppable          | Estimated droppable tombstones                            |
| Repaired At        | Time when marked as repaired by incremental repair        |


## ic-pstats ##
Tool for finding largest partitions. Reads the Index.db files so is relatively quick.

### Usage ###

    ic-pstats [-n <num>] [-t <snapshot>] [-f <filter>] <keyspace> <column-family>

| -h         | Display help                                                                    |
|------------|---------------------------------------------------------------------------------|
| -b         | Batch mode. Uses progress indicator that is friendly for running in batch jobs. |
| -n <num>   | Number of partitions to display                                                 |
| -t <name>  | Snapshot to analyse. Snapshot is created if none is specified.                  |
| -f <files> | Comma separated list of Data.db sstables to filter on                           |

### Output ###
Summary: Summary statistics about partitions

| Column                                                                 | Description                                              |
|------------------------------------------------------------------------|----------------------------------------------------------|
| Count (Size)                                                           | Number of partition keys on this node                    |
| Total (Size)                                                           | Total uncompressed size of all partitions on this node   |
| Total (SSTable)                                                        | Number of sstables on this node                          |
| Minimum (Size)                                                         | Minimum uncompressed partition size                      |
| Minimum (SSTable)                                                      | Minimum number of sstables a partition belongs to        |
| Average (Size)                                                         | Average (mean) uncompressed partition size               |
| Average (SSTable)                                                      | Average (mean) number of sstables a partition belongs to |
| std dev. (Size)                                                        | Standard deviation of partition sizes                    |
| std dev. (SSTable)                                                     | Standard deviation of number of sstables for a partition |
| 50% (Size)                                                             | Estimated 50th percentile of partition sizes             |
| 50% (SSTable)                                                          | Estimated 50th percentile of sstables for a partition    |
| 75% (Size)                                                             | Estimated 75th percentile of partition sizes             |
| 75% (SSTable)                                                          | Estimated 75th percentile of sstables for a partition    |
| 90% (Size)                                                             | Estimated 90th percentile of partition sizes             |
| 90% (SSTable)                                                          | Estimated 90th percentile of sstables for a partition    |
| 95% (Size)                                                             | Estimated 95th percentile of partition sizes             |
| 95% (SSTable)                                                          | Estimated 95th percentile of sstables for a partition    |
| 99% (Size)                                                             | Estimated 99th percentile of partition sizes             |
| 99% (SSTable)                                                          | Estimated 99th percentile of sstables for a partition    |
| 99.9% (Size)                                                           | Estimated 99.9th percentile of partition sizes           |
| 99.9% (SSTable)                                                        | Estimated 99.9th percentile of sstables for a partition  |
| Maximum (Size)                                                         | Maximum uncompressed partition size                      |
| Maximum (SSTable)                                                      | Maximum number of sstables a partition belongs to        |

Largest partitions: The top N largest partitions

| Column                                                                 | Description                                              |
|------------------------------------------------------------------------|----------------------------------------------------------|
| Key                                                                    | The partition key                                        |
| Size                                                                   | Total uncompressed size of the partition                 |
| SSTable Count                                                          | Number of sstables that contain the partition            |

SSTable Leaders: The top N partitions that belong to the most sstables

| Column                                                                 | Description                                              |
|------------------------------------------------------------------------|----------------------------------------------------------|
| Key                                                                    | The partition key                                        |
| SSTable Count                                                          | Number of sstables that contain the partition            |
| Size                                                                   | Total uncompressed size of the partition                 |

SSTables: Metadata about sstables as it relates to partitions.

| Column                                                                 | Description                                              |
|------------------------------------------------------------------------|----------------------------------------------------------|
| SSTable                                                                | Data.db filename of SSTable                              |
| Size                                                                   | Uncompressed size                                        |
| Min Timestamp                                                          | Minimum cell timestamp in the sstable                    |
| Max Timestamp                                                          | Maximum cell timestamp in the sstable                    |
| Level                                                                  | Leveled Tiered Compaction level of sstable               |
| Partitions                                                             | Number of partition keys in the sstable                  |
| Avg Partition Size                                                     | Average uncompressed partition size in sstable           |
| Max Partition Size                                                     | Maximum uncompressed partition size in sstable           |

## ic-cfstats ##
Tool for getting detailed cell statistics that can help identify issues with data model.

### Usage ###

    ic-cfstats [-r <limit>] [-n <num>] [-t <snapshot>] [-f <filter>] <keyspace> <column-family>
| -h         | Display help                                                                    |
|------------|---------------------------------------------------------------------------------|
| -b         | Batch mode. Uses progress indicator that is friendly for running in batch jobs. |
| -r <limit> | Limit read throughput to ratelimit MB/s                                         |
| -n <num>   | Number of partitions to display                                                 |
| -t <name>  | Snapshot to analyse. Snapshot is created if none is specified.                  |
| -f <files> | Comma separated list of Data.db sstables to filter on                           |

### Output ###
Summary: Summary statistics about partitions

| Column                                       | Description                                              |
|----------------------------------------------|----------------------------------------------------------|
| Count (Size)                                 | Number of partition keys on this node                    |
| Rows (Size)                                  | Number of clustering rows                                |
| (deleted)                                    | Number of clustering row deletions                       |
| Total (Size)                                 | Total uncompressed size of all partitions on this node   |
| Total (SSTable)                              | Number of sstables on this node                          |
| Minimum (Size)                               | Minimum uncompressed partition size                      |
| Minimum (SSTable)                            | Minimum number of sstables a partition belongs to        |
| Average (Size)                               | Average (mean) uncompressed partition size               |
| Average (SSTable)                            | Average (mean) number of sstables a partition belongs to |
| std dev. (Size)                              | Standard deviation of partition sizes                    |
| std dev. (SSTable)                           | Standard deviation of number of sstables for a partition |
| 50% (Size)                                   | Estimated 50th percentile of partition sizes             |
| 50% (SSTable)                                | Estimated 50th percentile of sstables for a partition    |
| 75% (Size)                                   | Estimated 75th percentile of partition sizes             |
| 75% (SSTable)                                | Estimated 75th percentile of sstables for a partition    |
| 90% (Size)                                   | Estimated 90th percentile of partition sizes             |
| 90% (SSTable)                                | Estimated 90th percentile of sstables for a partition    |
| 95% (Size)                                   | Estimated 95th percentile of partition sizes             |
| 95% (SSTable)                                | Estimated 95th percentile of sstables for a partition    |
| 99% (Size)                                   | Estimated 99th percentile of partition sizes             |
| 99% (SSTable)                                | Estimated 99th percentile of sstables for a partition    |
| 99.9% (Size)                                 | Estimated 99.9th percentile of partition sizes           |
| 99.9% (SSTable)                              | Estimated 99.9th percentile of sstables for a partition  |
| Maximum (Size)                               | Maximum uncompressed partition size                      |
| Maximum (SSTable)                            | Maximum number of sstables a partition belongs to        |

Row Histogram: Histogram of number of rows per partition

| Column                                       | Description                                                          |
|----------------------------------------------|----------------------------------------------------------------------|
| Percentile                                   | Minimum, average, standard deviation (std dev.), percentile, maximum |                                                                |
| Count                                        | Estimated number of rows per partition for the given percentile      | 

Largest partitions: Partitions with largest uncompressed size 

| Column                                                        | Description                                                      |
|---------------------------------------------------------------|------------------------------------------------------------------|
| Key                                                           | The partition key                                                |
| Size                                                          | Total uncompressed size of the partition                         |
| Rows                                                          | Total number of clustering rows in the partition                 |
| (deleted)                                                     | Number of row deletions in the partition                         |
| Tombstones                                                    | Number of cell or range tombstones                               |
| (droppable)                                                   | Number of tombstones that can be dropped as per gc_grace_seconds |
| Cells                                                         | Number of cells in the partition                                 |
| SSTable Count                                                 | Number of sstables that contain the partition                    |

Widest partitions: Partitions with the most cells

| Column                                            | Description                                                      |
|---------------------------------------------------|------------------------------------------------------------------|
| Key                                               | The partition key                                                |
| Rows                                              | Total number of clustering rows in the partition                 |
| (deleted)                                         | Number of row deletions in the partition                         |
| Cells                                             | Number of cells in the partition                                 |
| Tombstones                                        | Number of cell or range tombstones                               |
| (droppable)                                       | Number of tombstones that can be dropped as per gc_grace_seconds |
| Size                                              | Total uncompressed size of the partition                         |
| SSTable Count                                     | Number of sstables that contain the partition                    |

Most Deleted Rows: Partitions with the most row deletions

| Column                                            | Description                                                      |
|---------------------------------------------------|------------------------------------------------------------------|
| Key                                               | The partition key                                                |
| Rows                                              | Total number of clustering rows in the partition                 |
| (deleted)                                         | Number of row deletions in the partition                         |
| Size                                              | Total uncompressed size of the partition                         |
| SSTable Count                                     | Number of sstables that contain the partition                    |


Tombstone Leaders: Partitions with the most tombstones

| Column                                                 | Description                                                      |
|--------------------------------------------------------|------------------------------------------------------------------|
| Key                                                    | The partition key                                                |
| Tombstones                                             | Number of cell or range tombstones                               |
| (droppable)                                            | Number of tombstones that can be dropped as per gc_grace_seconds |
| Rows                                                   | Total number of clustering rows in the partition                 |
| Cells                                                  | Number of cells in the partition                                 |
| Size                                                   | Total uncompressed size of the partition                         |
| SSTable Count                                          | Number of sstables that contain the partition                    |

SSTable Leaders: Partitions that are in the most sstables

| Column                                                    | Description                                                      |
|-----------------------------------------------------------|------------------------------------------------------------------|
| Key                                                       | The partition key                                                |
| SSTable Count                                             | Number of sstables that contain the partition                    |
| Size                                                      | Total uncompressed size of the partition                         |
| Rows                                                      | Total number of clustering rows in the partition                 |
| Cells                                                     | Number of cells in the partition                                 |
| Tombstones                                                | Number of cell or range tombstones                               |
| (droppable)                                               | Number of tombstones that can be dropped as per gc_grace_seconds |

SSTables: Metadata about sstables as it relates to partitions. 

| Column                                                         | Description                                                                                                                                                        |
|----------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SSTable                                                        | Data.db filename of SSTable                                                                                                                                        |
| Size                                                           | Uncompressed size                                                                                                                                                  |
| Min Timestamp                                                  | Minimum cell timestamp in the sstable                                                                                                                              |
| Max Timestamp                                                  | Maximum cell timestamp in the sstable                                                                                                                              |
| Partitions                                                     | Number of partitions                                                                                                                                               |
| (deleted)                                                      | Number of row level partition deletions                                                                                                                            |
| (avg size)                                                     | Average uncompressed partition size in sstable                                                                                                                     |
| (max size)                                                     | Maximum uncompressed partition size in sstable                                                                                                                     |
| Rows                                                           | Total number of clustering rows in sstable                                                                                                                         |
| (deleted)                                                      | Number of row deletions in sstable                                                                                                                                 |
| Cells                                                          | Number of cells in the SSTable                                                                                                                                     |
| Tombstones                                                     | Number of cell or range tombstones in the SSTable                                                                                                                  |
| (droppable)                                                    | Number of tombstones that are droppable according to gc_grace_seconds                                                                                              |
| (range)                                                        | Number of range tombstones                                                                                                                                         |
| Cell Liveness                                                  | Percentage of live cells. Does not consider tombstones or cell updates shadowing cells. That is it is percentage of non-tombstoned cells to total number of cells. |

## ic-purge ##
Finds the largest reclaimable partitions (GCable). Intensive process, effectively does "fake" compactions to calculate metrics.

### Usage ###

    ic-purge [-r <limit>] [-n <num>] [-t <snapshot>] [-f <filter>] <keyspace> <column-family>

| -h         | Display help                                                                    |
|------------|---------------------------------------------------------------------------------|
| -b         | Batch mode. Uses progress indicator that is friendly for running in batch jobs. |
| -r <limit> | Limit read throughput to ratelimit MB/s                                         |
| -n <num>   | Number of partitions to display                                                 |
| -t <name>  | Snapshot to analyse. Snapshot is created if none is specified.                  |

### Output ###
Largest reclaimable partitions: Partitions with the largest amount of reclaimable data

| Column                                                                                 | Description                                  |
|----------------------------------------------------------------------------------------|----------------------------------------------|
| Key                                                                                    | The partition key                            |
| Size                                                                                   | Total uncompressed size of the partition     |
| Reclaim                                                                                | Reclaimable uncompressed size                |
| Generations                                                                            | SSTable generations the partition belongs to |


Please see https://www.instaclustr.com/support/documentation/announcements/instaclustr-open-source-project-status/ for Instaclustr support status of this project

