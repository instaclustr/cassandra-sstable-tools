package com.instaclustr.sstabletools;

import java.util.Arrays;

/**
 * Histogram snapshot.
 */
public class Snapshot {
    /**
     * Number of values.
     */
    protected final int count;

    /**
     * Sampled values.
     */
    protected final long[] values;

    /**
     * The minimum value recorded.
     */
    protected final long min;

    /**
     * The maximum value recorded.
     */
    protected final long max;

    /**
     * The total of all values.
     */
    protected final long total;

    protected Snapshot(long[] values, long min, long max, long total, int count) {
        this.values = values;
        Arrays.sort(this.values);
        this.min = min;
        this.max = max;
        this.total = total;
        this.count = count;
    }

    /**
     * Get the minimum value.
     *
     * @return the minimum value.
     */
    public long getMin() {
        return min;
    }

    /**
     * Get the maximum value.
     *
     * @return the maximum value.
     */
    public long getMax() {
        return max;
    }

    /**
     * Returns the average value.
     *
     * @return the average value
     */
    public double getMean() {
        return total / (double) count;
    }

    /**
     * Get the total of recorded values.
     *
     * @return the total of all values
     */
    public long getTotal() {
        return total;
    }

    /**
     * Returns the value at the given percentile.
     *
     * @param percentile a given percentile, in {@code [0..1]}
     * @return the value in the distribution at {@code percentile}
     */
    public double getPercentile(double percentile) {
        if (percentile < 0.0 || percentile > 1.0 || Double.isNaN(percentile)) {
            throw new IllegalArgumentException(percentile + " is not in [0..1]");
        }

        if (values.length == 0) {
            return 0.0;
        }

        final double pos = percentile * (values.length + 1);
        final int index = (int) pos;

        if (index < 1) {
            return values[0];
        }

        if (index >= values.length) {
            return values[values.length - 1];
        }

        final double lower = values[index - 1];
        final double upper = values[index];
        return lower + (pos - Math.floor(pos)) * (upper - lower);
    }

    /**
     * Returns the standard deviation of the values.
     *
     * @return the standard deviation value
     */
    public double getStdDev() {
        if (values.length <= 1) {
            return 0;
        }

        final double mean = getMean();
        double sum = 0;
        for (long value : values) {
            final double diff = value - mean;
            sum += diff * diff;
        }

        final double variance = sum / (values.length - 1);
        return Math.sqrt(variance);
    }
}
