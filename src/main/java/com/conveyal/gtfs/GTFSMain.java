package com.conveyal.gtfs;

import com.conveyal.gtfs.stats.FeedStats;
import com.conveyal.gtfs.util.json.JsonManager;
import com.conveyal.gtfs.util.json.JsonViews;
import com.conveyal.gtfs.validator.model.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

public class GTFSMain {

    private static final Logger LOG = LoggerFactory.getLogger(GTFSMain.class);

    public static void main (String[] args) throws Exception {
        Options options = getOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);
        if (cmd.hasOption("help")) {
            printHelp(options);
            return;
        }
        if (args.length < 1) {
            System.out.println("Please specify a GTFS feed to load.");
            System.exit(1);
        }
        File tempFile = File.createTempFile("gtfs", ".db");

        GTFSFeed feed = new GTFSFeed(tempFile.getAbsolutePath());
        feed.loadFromFile(new ZipFile(args[0]));

        feed.findPatterns();
        // TODO: add way to run only specific validators
        if(cmd.hasOption("validate")) {
            feed.validate();
            JsonManager<ValidationResult> json = new JsonManager(ValidationResult.class, JsonViews.Validation.class);
            ValidationResult result = new ValidationResult(args[0], feed, new FeedStats(feed));
            File resultFile;
            if (args.length >= 2) {
                resultFile = new File(args[1]);
            } else {
                resultFile = File.createTempFile("result", ".json");
            }
            FileUtils.writeStringToFile(resultFile, json.write(result));
            LOG.info("Storing validation result at: {}", resultFile.getAbsolutePath());
        }
        feed.close();

        LOG.info("reopening feed");

        // re-open
        GTFSFeed reconnected = new GTFSFeed(tempFile.getAbsolutePath());

        LOG.info("Connected to already loaded feed");

        LOG.info("  {} routes", reconnected.routes.size());
        LOG.info("  {} trips", reconnected.trips.size());
        LOG.info("  {} stop times", reconnected.stop_times.size());
        LOG.info("  Feed ID: {}", reconnected.feedId);
    }

    private static void printHelp(Options options) {
        final String HELP = String.join("\n",
                "java -jar gtfs-lib-shaded.jar [options] INPUT.zip [result.json]",
                "Load input GTFS feed into storage-backed MapDB for use in Java-based",
                "applications. Optionally, write feed to output location. For more, see",
                "https://github.com/conveyal/gtfs-lib#gtfs-lib",
                "", // blank lines for legibility
                ""
        );
        HelpFormatter formatter = new HelpFormatter();
        System.out.println(); // blank line for legibility
        formatter.printHelp( HELP, options );
        System.out.println(); // blank line for legibility
    }

    private static Options getOptions () {
        Options options = new Options();
        Option help = new Option("help", false, "print this message");
        Option validate = new Option("validate", false, "run full validation suite on input GTFS feed (optionally store at [result.json])");
        options.addOption(help);
        options.addOption(validate);
        return options;
    }

}
