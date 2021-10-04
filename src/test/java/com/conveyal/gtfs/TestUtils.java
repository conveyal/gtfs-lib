package com.conveyal.gtfs;

import com.conveyal.gtfs.error.NewGTFSErrorType;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.conveyal.gtfs.util.Util.randomIdString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    private static final String PG_URL = "jdbc:postgresql://localhost/postgres";
    public static final String PG_TEST_USER = "postgres";
    public static final String PG_TEST_PASSWORD = "postgres";

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
        if(executeAndClose(String.format("DROP DATABASE %s", dbName))) {
            LOG.error(String.format("Successfully dropped database: %s", dbName));
        } else {
            LOG.error(String.format("Failed to drop database: %s", dbName));
        }
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
            // This connection must be established without GTFS#createDataSource because the "create database" command
            // cannot run inside a transaction block.
            connection = DriverManager.getConnection(PG_URL, PG_TEST_USER, PG_TEST_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error("Error connecting to database!");
            return false;
        }

        try {
            LOG.info(statement);
            connection.prepareStatement(statement).execute();
        } catch (SQLException e) {
            e.printStackTrace();
            LOG.error(String.format("Error executing statement: %s", statement));
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

    public static DataSource createTestDataSource (String dbUrl) {
        return GTFS.createDataSource(dbUrl, PG_TEST_USER, PG_TEST_PASSWORD);
    }

    /**
     * Generate a new database for isolating a test.
     *
     * @return The name of the name database, or null if creation unsuccessful
     */
    public static String generateNewDB() {
        String newDBName = String.format("test_db_%s", randomIdString());
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
     * Zip files in a folder into a temporary zip file
     */
    public static String zipFolderFiles(String folderName, boolean isRelativeToResourcesPath) throws IOException {
        // create temporary zip file
        File tempFile = File.createTempFile("temp-gtfs-zip-", ".zip");
        tempFile.deleteOnExit();
        String tempFilePath = tempFile.getAbsolutePath();
        // If folder name is relative to resources path, get full path. Otherwise, it is assumed to be an absolute path.
        String folderPath = isRelativeToResourcesPath ? getResourceFileName(folderName) : folderName;
        // Do not nest files under a subdirectory if directly zipping a folder in src/main/resources
        compressZipfile(folderPath, tempFilePath, !isRelativeToResourcesPath);
        return tempFilePath;
    }

    private static void compressZipfile(String sourceDir, String outputFile, boolean nestDirectory) throws IOException {
        ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(outputFile));
        compressDirectoryToZipfile(sourceDir, sourceDir, zipFile, nestDirectory);
        IOUtils.closeQuietly(zipFile);
    }

    /**
     * Convenience method for zipping a directory.
     * @param nestDirectory whether nested folders should be preserved as subdirectories
     */
    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream out, boolean nestDirectory) throws IOException {
        for (File file : new File(sourceDir).listFiles()) {
            if (file.isDirectory()) {
                compressDirectoryToZipfile(rootDir, fileNameWithDir(sourceDir, file.getName()), out, nestDirectory);
            } else {
                String folderName = sourceDir.replace(rootDir, "");
                String zipEntryName = nestDirectory
                    ? fileNameWithDir(folderName, file.getName())
                    : String.join("", folderName, file.getName());
                ZipEntry entry = new ZipEntry(zipEntryName);
                out.putNextEntry(entry);

                FileInputStream in = new FileInputStream(file.getAbsolutePath());
                IOUtils.copy(in, out);
                IOUtils.closeQuietly(in);
            }
        }
    }

    public static String fileNameWithDir(String directory, String filename) {
        return String.join(File.separator, directory, filename);
    }

    /**
     * Asserts that the result of a SQL count statement is equal to an expected value
     *
     * @param sql A SQL statement in the form of `SELECT count(*) FROM ...`
     * @param expectedCount The expected count that is returned from the result of the SQL statement.
     */
    public static void assertThatSqlCountQueryYieldsExpectedCount(DataSource dataSource, String sql, int expectedCount) {
        int count = -1;
        LOG.info(sql);
        // Encapsulate connection in try-with-resources to ensure it is closed and does not interfere with other tests.
        try (Connection connection = dataSource.getConnection()) {
            ResultSet resultSet = connection.prepareStatement(sql).executeQuery();
            if (resultSet.next()) {
                count = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            LOG.error("SQL error encountered", e);
        }
        assertThat(
            "Records matching query should equal expected count.",
            count,
            equalTo(expectedCount)
        );
    }

    /**
     * Check that the test feed has expected number of errors for the provided values.
     */
    public static void checkFeedHasExpectedNumberOfErrors(
        String testNamespace,
        DataSource testDataSource,
        NewGTFSErrorType errorType,
        String entityType,
        String lineNumber,
        String entityId,
        String badValue,
        int expectedNumberOfErrors
    ) {
        String sql = String.format("select count(*) from %s.errors where error_type = '%s' and entity_type = '%s' and line_number = '%s'",
            testNamespace,
            errorType,
            entityType,
            lineNumber);

        if (entityId != null) sql += String.format(" and entity_id = '%s'", entityId);
        if (badValue != null) sql += String.format(" and bad_value = '%s'", badValue);

        assertThatSqlCountQueryYieldsExpectedCount(testDataSource, sql, expectedNumberOfErrors);
    }


}
