package com.instaclustr.sstabletools.cli;

import picocli.CommandLine;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

public abstract class CLIApplication implements CommandLine.IVersionProvider {

    public static int execute(final Runnable runnable, String... args) {
        return execute(new CommandLine(runnable), args);
    }

    public static int execute(final Callable callable, String... args) {
        return execute(new CommandLine(callable), args);
    }

    public static int execute(CommandLine commandLine, String... args) {
        return commandLine
                .setErr(new PrintWriter(System.err, true))
                .setOut(new PrintWriter(System.out, true))
                .setColorScheme(new CommandLine.Help.ColorScheme.Builder().ansi(CommandLine.Help.Ansi.ON).build())
                .setExecutionExceptionHandler((ex, cmdLine, parseResult) -> {
                    ex.printStackTrace();
                    return 1;
                })
                .execute(args);
    }

    public abstract String title();

    @Override
    public String[] getVersion() throws Exception {
        return VersionParser.parse(title());
    }
}
