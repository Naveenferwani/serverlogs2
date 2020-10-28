package com.serverlogs.application.repository;

import com.serverlogs.application.dto.ServerLogs;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.List;

public class ServerLogsRepository {

    private final Logger LOG = Logger.getLogger(ServerLogsRepository.class.getName());

    /**
     * Method to create table and Insert Log events
     *
     * @param serverLogs - Server Logs List
     * @throws SQLException
     */
    public void processServerLogs(List<ServerLogs> serverLogs) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:hsqldb:file:serverlogsdb", "SA", "");
        createTableIfNotExits(connection);
        insertRecords(serverLogs, connection);
        connection.commit();
    }

    /**
     * Method to create Table if not AlreadyExists
     *
     * @param connection
     * @throws SQLException
     */
    private void createTableIfNotExits(Connection connection) throws SQLException {
        DatabaseMetaData dbm = connection.getMetaData();
        ResultSet tables = dbm.getTables(null, null, "SERVER_LOGS", null);
        if (!tables.next()) {
            LOG.info("Table Doesn't Exists.. Creating SERVER_LOGS Table");
            String tableScript = "CREATE TABLE SERVER_LOGS (EVENT_ID VARCHAR(15), DURATION INT, TYPE VARCHAR(15), HOST VARCHAR(20), ALERT BOOLEAN);";
            Statement statement = connection.createStatement();
            statement.execute(tableScript);
            LOG.info("Table Created Successfully");
        }
    }

    /**
     * Method to Insert Log Event
     *
     * @param serverLogsList
     * @param connection
     * @throws SQLException
     */
    private void insertRecords(List<ServerLogs> serverLogsList, Connection connection) throws SQLException {
        String INSERT_SQL = "INSERT INTO SERVER_LOGS VALUES(?, ?, ?, ?, ?);";
        int count = 0;
        for (ServerLogs serverLogs : serverLogsList) {
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL);
            preparedStatement.setString(1, serverLogs.getId());
            preparedStatement.setLong(2, serverLogs.getDuration());
            preparedStatement.setString(3, serverLogs.getType());
            preparedStatement.setString(4, serverLogs.getHost());
            preparedStatement.setBoolean(5, serverLogs.isAlert());
            count += preparedStatement.executeUpdate();
        }
        LOG.debug("Number of records Inserted: " + count);
    }
}
