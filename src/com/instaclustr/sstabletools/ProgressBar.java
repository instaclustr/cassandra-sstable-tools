package com.instaclustr.sstabletools;

import com.google.common.base.Strings;

/**
 * Progress bar.
 */
public final class ProgressBar {
    private static String FULL_BAR = Strings.repeat("█", 30);
    private static String EMPTY_BAR = Strings.repeat("░", 30);

    /**
     * Progress bar title.
     */
    private String title;

    /**
     * Last percentage that was displayed.
     */
    private int lastPercentage;

    /**
     * Flag to indicate if interactive console.
     */
    private boolean interactive;

    /**
     * Time in milliseconds when progress bar was started.
     */
    private long startTime;

    /**
     * Construct progress bar.
     *
     * @param title Progress bar title
     */
    public ProgressBar(String title, boolean interactive) {
        this.title = title;
        this.lastPercentage = -1;
        this.interactive = interactive;
        this.startTime = System.currentTimeMillis();
    }

    /**
     * Update the progress.
     *
     * @param percentComplete Percentage completed.
     */
    public void updateProgress(double percentComplete) {
        if (this.lastPercentage == 100) {
            return;
        }
        int percentage = (int) (percentComplete * 100);
        if (percentage != this.lastPercentage) {
            this.lastPercentage = percentage;
            long elapsedTime = System.currentTimeMillis() - startTime;
            long eta = Math.round(Math.ceil(elapsedTime / percentComplete - elapsedTime));
            String strETA = Util.humanReadableDateDiff(0, eta);
            if (this.interactive) {
                if (percentage == 100) {
                    String bar = String.format(
                            "\033[2K\r%s %s (%s%%)",
                            title,
                            FULL_BAR,
                            percentage
                    );
                    System.out.println(bar);
                    System.out.println();
                } else {
                    int cols = (int) (percentComplete * 30);
                    String bar = String.format(
                            "\033[2K\r%s %s%s (%s%%) ETA: %s",
                            title,
                            FULL_BAR.substring(30 - cols),
                            EMPTY_BAR.substring(cols),
                            percentage,
                            strETA
                    );
                    System.out.print(bar);
                    System.out.flush();
                }
            } else {
                if (percentage == 100) {
                    System.out.println(String.format("%s (%s%%)", title, percentage));
                    System.out.println();
                } else {
                    System.out.println(String.format("%s (%s%%) ETA: %s", title, percentage, strETA));
                }
            }
        }
    }
}
