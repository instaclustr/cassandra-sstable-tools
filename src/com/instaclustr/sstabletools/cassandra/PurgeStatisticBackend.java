package com.instaclustr.sstabletools.cassandra;

import com.google.common.util.concurrent.RateLimiter;
import com.instaclustr.sstabletools.PurgeStatistics;
import com.instaclustr.sstabletools.PurgeStatisticsReader;
import com.instaclustr.sstabletools.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.LazilyCompactedRow;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Column statistic reader backend.
 */
public class PurgeStatisticBackend implements PurgeStatisticsReader {
    /**
     * A queue of scanners sorted by key.
     */
    private PriorityQueue<ScannerWrapper> readerQueue;

    /**
     * Bytes read so far.
     */
    private long bytesRead;

    /**
     * Total uncompressed length of sstables.
     */
    private long length;

    /**
     * Compaction controller.
     */
    private CompactionController controller;

    public PurgeStatisticBackend(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, RateLimiter rateLimiter) {
        bytesRead = 0;
        readerQueue = new PriorityQueue<>(sstables.isEmpty() ? 1 : sstables.size());
        for (SSTableReader sstable : sstables) {
            length += sstable.uncompressedLength();
            ScannerWrapper scanner = new ScannerWrapper(sstable.descriptor.generation, sstable.getScanner(rateLimiter));
            if (scanner.next()) {
                readerQueue.add(scanner);
            }
        }
        this.controller = new CompactionController(cfs, null, cfs.gcBefore(Util.NOW));
    }
    
    public double getProgress() {
        return bytesRead / (double) length;
    }

    @Override
    public boolean hasNext() {
        return !readerQueue.isEmpty();
    }

    @Override
    public PurgeStatistics next() {
        if (readerQueue.isEmpty()) {
            return null;
        }

        PurgeStatistics stats = new PurgeStatistics();

        // Grab row with lowest key.
        List<ScannerWrapper> scanners = new ArrayList<>(readerQueue.size());
        ScannerWrapper scanner = readerQueue.remove();
        scanners.add(scanner);
        stats.key = scanner.row.getKey();

        // Grab matching rows from other scanners.
        while ((scanner = this.readerQueue.peek()) != null && scanner.row.getKey().equals(stats.key)) {
            readerQueue.remove();
            scanners.add(scanner);
        }

        // List of rows from each scanner.
        List<OnDiskAtomIterator> rows = new ArrayList<>(scanners.size());
        for (ScannerWrapper s: scanners) {
            rows.add(s.row);
            stats.generations.add(s.generation);
        }

        NullDataOutput out = new NullDataOutput();
        LazilyCompactedRow compactedRow = new LazilyCompactedRow(controller, rows);
        try {
            compactedRow.write(out.getCurrentPosition(), out);
        } catch (IOException e) {}
        long mergedSize = out.getCurrentPosition();

        // Increment scanners.
        for (ScannerWrapper s : scanners) {
            stats.size += s.partitionSize();
            if (s.next()) {
                readerQueue.add(s);
            }
        }
        bytesRead += stats.size;

        stats.reclaimable = stats.size - mergedSize;

        return stats;
    }

    private class ScannerWrapper implements Comparable<ScannerWrapper> {
        /**
         * Generation of sstable being scanned.
         */
        public int generation;

        /**
         * SSTable scanner.
         */
        private ISSTableScanner scanner;

        /**
         * The current row.
         */
        public SSTableIdentityIterator row;

        /**
         * Position in Data.db file.
         */
        private long position;

        public ScannerWrapper(int generation, ISSTableScanner scanner) {
            this.generation = generation;
            this.scanner = scanner;
            this.position = 0;
        }

        public boolean next() {
            if (!scanner.hasNext()) {
                return false;
            }
            this.row = (SSTableIdentityIterator) scanner.next();
            return true;
        }

        public long partitionSize() {
            long currentPosition = scanner.getCurrentPosition();
            long size = currentPosition - position;
            position = currentPosition;
            return size;
        }

        @Override
        public int compareTo(ScannerWrapper o) {
            return this.row.getKey().compareTo(o.row.getKey());
        }
    }

    /**
     * DataOutputPlus that
     */
    private class NullDataOutput implements DataOutputPlus {
        private long position = 0;

        public long getCurrentPosition() {
            return position;
        }

        @Override
        public void write(ByteBuffer byteBuffer) throws IOException {
            position += byteBuffer.remaining();
        }

        @Override
        public void write(Memory memory, long offset, long length) throws IOException {
            for (ByteBuffer buffer : memory.asByteBuffers(offset, length)) {
                write(buffer);
            }
        }

        @Override
        public void write(int b) throws IOException {
            position += 4;
        }

        @Override
        public void write(byte[] b) throws IOException {
            position += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            position += len;
        }

        @Override
        public void writeBoolean(boolean v) throws IOException {
            position++;
        }

        @Override
        public void writeByte(int v) throws IOException {
            position++;
        }

        @Override
        public void writeShort(int v) throws IOException {
            position += 2;
        }

        @Override
        public void writeChar(int v) throws IOException {
            position += 2;
        }

        @Override
        public void writeInt(int v) throws IOException {
            position += 4;
        }

        @Override
        public void writeLong(long v) throws IOException {
            position += 8;
        }

        @Override
        public void writeFloat(float v) throws IOException {
            position += 4;
        }

        @Override
        public void writeDouble(double v) throws IOException {
            position += 8;
        }

        @Override
        public void writeBytes(String s) throws IOException {
            position += s.length();
        }

        @Override
        public void writeChars(String s) throws IOException {
            position += s.length() * 2;
        }

        @Override
        public void writeUTF(String str) throws IOException {
            int utfCount = 0, length = str.length();
            for (int i = 0; i < length; i++) {
                int charValue = str.charAt(i);
                if (charValue > 0 && charValue <= 127) {
                    utfCount++;
                } else if (charValue <= 2047) {
                    utfCount += 2;
                } else {
                    utfCount += 3;
                }
            }
            if (utfCount > 65535) {
                throw new UTFDataFormatException(); //$NON-NLS-1$
            }
            position += utfCount * 2;
        }
    }
}
