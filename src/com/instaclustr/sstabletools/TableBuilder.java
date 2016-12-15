package com.instaclustr.sstabletools;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Tabulate output.
 */
public class TableBuilder {
    private String[] header = null;
    private List<String[]> rows = new ArrayList<>();

    public void setHeader(String... headings) {
        header = headings;
    }

    public void addRow(String... cols) {
        rows.add(cols);
    }

    private int[] colWidths() {
        int cols = header.length;
        for (String[] row : rows) {
            cols = Math.max(cols, row.length);
        }
        int[] widths = new int[cols];
        if (header != null) {
            for (int colNum = 0; colNum < header.length; colNum++) {
                widths[colNum] = Math.max(widths[colNum], StringUtils.length(header[colNum]));
            }
        }
        for (String[] row : rows) {
            for (int colNum = 0; colNum < row.length; colNum++) {
                widths[colNum] = Math.max(widths[colNum], StringUtils.length(row[colNum]));
            }
        }
        return widths;
    }

    private static void rowSeparator(StringBuilder buf, int[] colWidths) {
        buf.append('+');
        for (int colNum = 0; colNum < colWidths.length; colNum++) {
            buf.append(StringUtils.repeat('-', colWidths[colNum] + 2));
            buf.append('+');
        }
        buf.append('\n');
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        int[] colWidths = colWidths();
        rowSeparator(buf, colWidths);
        if (header != null) {
            buf.append('|');
            for (int colNum = 0; colNum < header.length; colNum++) {
                buf.append(' ');
                buf.append(StringUtils.rightPad(StringUtils.defaultString(header[colNum]), colWidths[colNum]));
                buf.append(" |");
            }
            buf.append('\n');
            rowSeparator(buf, colWidths);
        }
        for (String[] row : rows) {
            buf.append('|');
            for (int colNum = 0; colNum < row.length; colNum++) {
                buf.append(' ');
                if (colNum > 0) {
                    buf.append(StringUtils.leftPad(StringUtils.defaultString(row[colNum]), colWidths[colNum]));
                } else {
                    buf.append(StringUtils.rightPad(StringUtils.defaultString(row[colNum]), colWidths[colNum]));
                }
                buf.append(" |");
            }
            buf.append('\n');
        }
        rowSeparator(buf, colWidths);
        return buf.toString();
    }
}
