package com.instaclustr.sstabletools.cli;

import picocli.CommandLine;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;

public class VersionParser {
    public static String[] parse(String title) throws IOException
    {
        Enumeration<URL> resources = CommandLine.class.getClassLoader().getResources("git.properties");

        Optional<String> implementationVersion = Optional.empty();
        Optional<String> buildTime = Optional.empty();
        Optional<String> gitCommit = Optional.empty();

        while (resources.hasMoreElements()) {
            final URL url = resources.nextElement();

            Properties properties = new Properties();
            properties.load(url.openStream());

            if (properties.getProperty("git.build.time") != null) {
                implementationVersion = Optional.ofNullable(properties.getProperty("git.build.version"));
                buildTime = Optional.ofNullable(properties.getProperty("git.build.time"));
                gitCommit = Optional.ofNullable(properties.getProperty("git.commit.id.full"));
            }
        }

        return new String[]{
                String.format("%s %s", title, implementationVersion.orElse("development build")),
                String.format("Build time: %s", buildTime.orElse("unknown")),
                String.format("Git commit: %s", gitCommit.orElse("unknown")),
        };
    }

}
