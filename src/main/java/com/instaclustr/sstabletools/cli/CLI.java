package com.instaclustr.sstabletools.cli;

import java.io.PrintWriter;

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
public class CLI extends JarManifestVersionProvider implements Runnable {

    @Spec
    private CommandSpec spec;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void main(String[] args, boolean exit) {
        int exitCode = new CommandLine(new CLI())
            .setErr(new PrintWriter(System.err))
            .setOut(new PrintWriter(System.err))
            .setColorScheme(new CommandLine.Help.ColorScheme.Builder().ansi(CommandLine.Help.Ansi.ON).build())
            .setExecutionExceptionHandler((ex, cmdLine, parseResult) -> {
                ex.printStackTrace();
                return 1;
            })
            .execute(args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    @Override
    public String getImplementationTitle() {
        return "ic-sstable-tools";
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required sub-command.");
    }
}

