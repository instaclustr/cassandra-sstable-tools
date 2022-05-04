package com.instaclustr.sstabletools;

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
        if (count < Integer.MAX_VALUE - 1) {
            count++;
        }
        total += value;
        min = Math.min(value, min);
        max = Math.max(value, max);
    }

    protected int size() {
        if (count > reservoir.length) {
            return reservoir.length;
        }
        return count;
    }

    /**
     * @return snapshot histogram.
     */
    public Snapshot snapshot() {
        final int s = size();
        long[] copy = new long[s];
        for (int i = 0; i < s; i++) {
            copy[i] = reservoir[i];
        }
        return new Snapshot(copy, min, max, total, count);
    }
}
