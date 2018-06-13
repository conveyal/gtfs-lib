package com.conveyal.gtfs;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    private static final AtomicInteger UNIQUE_ID = new AtomicInteger(0);
    private static String pgUrl = "jdbc:postgresql://localhost/postgres";

    /**
     * Forcefully drops a database even if other users are connected to it.
     *
     * @param dbName
     */
    public static void dropDB(String dbName) {
        // first, terminate all other user sessions
        executeAndClose(String.format("SELECT pg_terminate_backend(pg_stat_activity.pid) " +
            "FROM pg_stat_activity " +
            "WHERE pg_stat_activity.datname = '%s' " +
            "AND pid <> pg_backend_pid()", dbName
        ));
        // drop the db
        executeAndClose(String.format("DROP DATABASE %s", dbName));
    }

    /**
     * Boilerplate for opening a connection, executing a statement and closing connection.
     *
     * @param statement
     * @return true if everything worked.
     */
    private static boolean executeAndClose(String statement) {
        Connection connection;
        try {
            connection = DriverManager.getConnection(pgUrl);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error connecting to database!");
            return false;
        }

        try {
            connection.prepareStatement(statement).execute();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error creating new database!");
            return false;
        }

        try {
            connection.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error closing connection!");
            return false;
        }
    }

    /**
     * Generate a new database for isolating a test.
     *
     * @return The name of the name database, or null if creation unsuccessful
     */
    public static String generateNewDB() {
        String newDBName = uniqueString();
        if (executeAndClose(String.format("CREATE DATABASE %s", newDBName))) {
            return newDBName;
        } else {
            return null;
        }
    }

    /**
     * Helper to return the relative path to a test resource file
     *
     * @param fileName
     * @return
     */
    public static String getResourceFileName(String fileName) {
        return String.format("./src/test/resources/%s", fileName);
    }

    /**
     * Generate a unique string.  Mostly copied from the uniqueId method of https://github.com/javadev/underscore-java
     */
    public static String uniqueString() {
        return String.format("test_db_%d", UNIQUE_ID.incrementAndGet());
    }

    /**
     * Zip files in a folder into a temporary zip file
     */
    public static String zipFolderFiles(String folderName) throws IOException {
        // create temporary zip file
        File tempFile = File.createTempFile("temp-gtfs-zip-", ".zip");
        tempFile.deleteOnExit();
        String tempFilePath = tempFile.getAbsolutePath();
        compressZipfile(TestUtils.getResourceFileName(folderName), tempFilePath);
        return tempFilePath;
    }

    private static void compressZipfile(String sourceDir, String outputFile) throws IOException, FileNotFoundException {
        ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(outputFile));
        compressDirectoryToZipfile(sourceDir, sourceDir, zipFile);
        IOUtils.closeQuietly(zipFile);
    }

    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream out) throws IOException, FileNotFoundException {
        for (File file : new File(sourceDir).listFiles()) {
            if (file.isDirectory()) {
                compressDirectoryToZipfile(rootDir, sourceDir + File.separator + file.getName(), out);
            } else {
                ZipEntry entry = new ZipEntry(sourceDir.replace(rootDir, "") + file.getName());
                out.putNextEntry(entry);

                FileInputStream in = new FileInputStream(file.getAbsolutePath());
                IOUtils.copy(in, out);
                IOUtils.closeQuietly(in);
            }
        }
    }
}
