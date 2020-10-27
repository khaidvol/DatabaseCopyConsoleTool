package com.epam.dcct;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatabaseOperator {


    private static final String CREATE_DATABASE = "CREATE DATABASE ";
    private static final String DROP_DATABASE = "DROP DATABASE IF EXISTS ";
    public static final String SHOW_TABLES = "SHOW TABLES";
    public static final String SHOW_TABLE_DEFINITION = "SHOW CREATE TABLE ";
    private static final String DISABLE_FOREIGN_KEY_CHECK = "SET FOREIGN_KEY_CHECKS=0";
    private static final String ENABLE_FOREIGN_KEY_CHECK = "SET FOREIGN_KEY_CHECKS=1";
    public static final String DONE = "DONE";

    public static final Logger logger = Logger.getRootLogger();

    public DatabaseOperator() {
    }

    public boolean testConnection(String db) {
        try (Connection connection = Datasource.getConnection(db)) {
            logger.info(db + " connected!");
        } catch (SQLException e) {
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }

    public void dropCopyDatabaseIfExist(String dbOriginal, String dbCopy) {
        try (Connection connection = Datasource.getConnection(dbOriginal);
             Statement statement = connection.createStatement()
        ) {
            logger.info("Drop database " + dbCopy + " if exist");
            statement.execute(DROP_DATABASE + dbCopy);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        logger.info(DONE);
    }

    public void createCopyDatabase(String dbOriginal, String dbCopy) {
        try (Connection connection = Datasource.getConnection(dbOriginal);
             Statement statement = connection.createStatement()
        ) {
            logger.info("Create new database " + dbCopy);
            statement.execute(CREATE_DATABASE + dbCopy);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        logger.info(DONE);
    }

    public List<String> getTables(String dbOriginal) {
        List<String> tables = new ArrayList<>();

        try (Connection connection = Datasource.getConnection(dbOriginal);
             Statement statement= connection.createStatement()
        ) {
            logger.info("Fetch list of tables...");
            ResultSet resultSet = statement.executeQuery(SHOW_TABLES);
            while(resultSet.next()) {
                tables.add(resultSet.getString(1));
                logger.info(resultSet.getString(1) + " - table added to the list");
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        logger.info(DONE);
        //sort in lexicographical order according to requirements
        Collections.sort(tables);

        return tables;
    }

    public String getTableDefinitionSQL(String dbOriginal, String tableName){
        String tableDefinition = null;
        try (Connection connection = Datasource.getConnection(dbOriginal);
             Statement statement = connection.createStatement()
        ) {
            logger.info("Get table definition SQL for " + tableName);
            String dbTable = dbOriginal + "." + tableName;
            ResultSet resultSet = statement.executeQuery(SHOW_TABLE_DEFINITION + dbTable);
            if(resultSet.next()) tableDefinition = resultSet.getString(2);

        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        logger.info(DONE);
        return tableDefinition;
    }

    public void createTable(String dbCopy, String tableDefinitionSQL) {
        try (Connection connection = Datasource.getConnection(dbCopy);
             Statement statement = connection.createStatement()
        ) {
            logger.info("Create table in " + dbCopy);
            statement.execute(DISABLE_FOREIGN_KEY_CHECK);
            statement.execute(tableDefinitionSQL);
            statement.execute(ENABLE_FOREIGN_KEY_CHECK);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        logger.info(DONE);
    }

    public void copyTable(String dbCopy, String dbOriginal, String tableName) {
        try (Connection connection = Datasource.getConnection(dbOriginal);
             Statement statement = connection.createStatement()
        ) {
            String dbCopyTable = dbCopy.concat(".").concat(tableName);
            String dbOriginalTable = dbOriginal.concat(".").concat(tableName);
            logger.info("Copy data from " + dbOriginalTable + " to " + dbCopy);

            statement.execute(DISABLE_FOREIGN_KEY_CHECK);
            statement.execute("INSERT INTO " + dbCopyTable +
                                        " SELECT * FROM " + dbOriginalTable);
            statement.execute(ENABLE_FOREIGN_KEY_CHECK);

        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        logger.info(DONE);
    }

}