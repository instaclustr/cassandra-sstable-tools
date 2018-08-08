package com.instaclustr.sstabletools;

import java.util.Arrays;
import java.util.Random;

/**
 * Implements a histogram using Algorithm R by Jeffrey Vitter. See https://en.wikipedia.org/wiki/Reservoir_sampling
 */
public class Histogram {
    /**
     * Default sampling size.
     */
    private static final int DEFAULT_SIZE = 1028;

    /**
     * Number of values recorded.
     */
    protected int count = 0;

    /**
     * Reservoir of values as per the Algorithm R.
     */
    protected long[] reservoir;

    /**
     * Random index generator.
     */
    protected final Random random = new Random();

    /**
     * The minimum value recorded.
     */
    protected long min = Long.MAX_VALUE;

    /**
     * The maximum value recorded.
     */
    protected long max = 0;

    /**
     * The total of all values.
     */
    protected long total = 0;

    public Histogram() {
        this(DEFAULT_SIZE);
    }

    public Histogram(int sampleSize) {
        this.reservoir = new long[sampleSize];
    }

    /**
     * Count of values recorded.
     *
     * @return number of values recorded
     */
    public int getCount() {
        return count;
    }

    /**
     * Update histogram with a value.
     *
     * @param value value to add
     */
    public void update(long value) {
        if (count < reservoir.length) {
            // fill the reservoir array
            reservoir[count] = value;
        } else {
            // replace elements with gradually decreasing probability
            int i = random.nextInt(count + 1);
            if (i < reservoir.length) {
                reservoir[i] = value;
            }
        }
        count++;
        total += value;
        min = Math.min(value, min);
        max = Math.max(value, max);
    }

    /**
     * Snapshot histogram.
     */
    public void snapshot() {
        Arrays.sort(reservoir);
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
     * Returns the value at the given quantile.
     *
     * Snapshot histogram before calling this method.
     *
     * @param quantile a given quantile, in {@code [0..1]}
     * @return the value in the distribution at {@code quantile}
     */
    public double getValue(double quantile) {
        if (quantile < 0.0 || quantile > 1.0 || Double.isNaN(quantile)) {
            throw new IllegalArgumentException(quantile + " is not in [0..1]");
        }

        if (reservoir.length == 0) {
            return 0.0;
        }

        final double pos = quantile * (reservoir.length + 1);
        final int index = (int) pos;

        if (index < 1) {
            return reservoir[0];
        }

        if (index >= reservoir.length) {
            return reservoir[reservoir.length - 1];
        }

        final double lower = reservoir[index - 1];
        final double upper = reservoir[index];
        return lower + (pos - Math.floor(pos)) * (upper - lower);
    }

    /**
     * Returns the standard deviation of the values.
     *
     * @return the standard deviation value
     */
    public double getStdDev() {
        if (reservoir.length <= 1) {
            return 0;
        }

        final double mean = getMean();
        double sum = 0;
        for (long value : reservoir) {
            final double diff = value - mean;
            sum += diff * diff;
        }

        final double variance = sum / (reservoir.length - 1);
        return Math.sqrt(variance);
    }
}
