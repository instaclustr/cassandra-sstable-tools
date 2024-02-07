package com.instaclustr.sstabletools.cli;

import com.instaclustr.sstabletools.PurgeStatisticsCollector;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
        mixinStandardHelpOptions = true,
        subcommands = {
                ColumnFamilyStatisticsCollector.class,
                PartitionSizeStatisticsCollector.class,
                PurgeStatisticsCollector.class,
                SSTableMetadataCollector.class,
                SummaryCollector.class,
        },
        versionProvider = CLI.class,
        usageHelpWidth = 128
)
public class CLI extends CLIApplication implements Runnable {

    @Spec
    private CommandSpec spec;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void mainWithoutExit(String[] args) {
        main(args, false);
    }

    public static void main(String[] args, boolean exit) {
        int exitCode = execute(new CommandLine(new CLI()), args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required sub-command.");
    }

    @Override
    public String title() {
        return "ic-sstable-tools";
    }
}

