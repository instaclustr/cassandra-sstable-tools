package com.instaclustr.sstabletools;

import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;

import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.TimeZone;

/**
 * Utility functions.
 */
public final class Util {

    public static final long NOW;
    public static final int NOW_SECONDS;
    public static final SimpleDateFormat UTC_DATE_FORMAT;

    private static final Random rand;

    static {
        NOW = System.currentTimeMillis();
        NOW_SECONDS = (int) (NOW / 1000);

        UTC_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        UTC_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));

        rand = new Random();
    }

    public static int compareIds(int cmp, SSTableId o1, SSTableId o2) {
        if (o1 instanceof UUIDBasedSSTableId) {
            return cmp == 0 ? ((UUIDBasedSSTableId) o1).compareTo((UUIDBasedSSTableId) o2) : cmp;
        } else if (o1 instanceof SequenceBasedSSTableId) {
            return cmp == 0 ? ((SequenceBasedSSTableId) o1).compareTo((SequenceBasedSSTableId) o2) : cmp;
        } else {
            throw new IllegalStateException("Unable to process SSTableId of type " + o1.getClass());
        }
    }

    public static String humanReadableByteCount(long bytes) {
        return humanReadableByteCount(bytes, true);
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static String generateString(Random rng, String characters, int length) {
        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = characters.charAt(rng.nextInt(characters.length()));
        }
        return new String(text);
    }

    public static String generateSnapshotName() {
        return "analyse-" + Util.generateString(rand, "ABCDEFGHIJKLMNOPQRSTUVWXYZ", 20);
    }

    public static String humanReadableDateDiff(long start, long end) {
        long diff = end - start;
        StringBuilder sb = new StringBuilder();
        long days = diff / (24 * 60 * 60 * 1000);
        diff -= days * 24 * 60 * 60 * 1000;
        if (days >= 1) {
            sb.append(days);
            sb.append("d");
        }

        long hours = diff / (60 * 60 * 1000);
        diff -= hours * 60 * 60 * 1000;
        if (hours >= 1) {
            if (sb.length() > 1) {
                sb.append(' ');
            }
            sb.append(hours);
            sb.append("h");
        }

        if (sb.length() > 1) {
            sb.append(' ');
        }

        long minutes = diff / (60 * 1000);
        diff -= minutes * 60 * 1000;
        sb.append(minutes);
        sb.append("m ");

        sb.append(Math.round(Math.ceil(diff / 1000.0)));
        sb.append("s");
        return sb.toString();
    }
}
