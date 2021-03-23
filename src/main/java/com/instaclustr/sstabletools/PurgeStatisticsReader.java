package com.instaclustr.sstabletools;

import java.util.Iterator;

/**
 * Reader for getting column statistic for a partition.
 */
public interface PurgeStatisticsReader extends Iterator<PurgeStatistics> {
    /**
     * Read progress.
     *
     * @return Read progress as percentage.
     */
    double getProgress();
}
