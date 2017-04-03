package com.conveyal.gtfs.storage;

import java.io.InputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by abyrd on 2017-03-27
 */
public class SqlLibrary {

    public Connection connection;

    private void example() throws Exception {
        String sql = "SELECT * FROM gtfs1234.stops";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, 1000);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next())
        {
            System.out.print("Column 1 returned ");
            System.out.println(resultSet.getString(1));
        }
        resultSet.close();
        preparedStatement.close();
    }

    public void executeSqlScript (String scriptName) {
        try {
            InputStream scriptStream = Backend.class.getResourceAsStream(scriptName);
            Scanner scanner = new Scanner(scriptStream).useDelimiter(";");
            while (scanner.hasNext()) {
                Statement currentStatement = connection.createStatement();
                currentStatement.execute(scanner.next());
            }
            scanner.close();
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
    }

    public Map<String, PreparedStatement> loadStatements (String scriptName) {
        Map<String, PreparedStatement> preparedStatements = new HashMap<>();
        try {
            InputStream scriptStream = Backend.class.getResourceAsStream(scriptName);
            Scanner statementScanner = new Scanner(scriptStream).useDelimiter(";");
            while (statementScanner.hasNext()) {
                String statement = statementScanner.next().trim();
                if (statement.startsWith("--")) {
                    Scanner tokenScanner = new Scanner(statement.substring(2));
                    if (tokenScanner.hasNext()) {
                        String firstToken = tokenScanner.next();
                        PreparedStatement preparedStatement = connection.prepareStatement(statement);
                        preparedStatements.put(firstToken, preparedStatement);
                    }
                }
            }
            statementScanner.close();
        } catch (Exception ex) {
            throw new StorageException(ex);
        }
        return preparedStatements;
    }

}
