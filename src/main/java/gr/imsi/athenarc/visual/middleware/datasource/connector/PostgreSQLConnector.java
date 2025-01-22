package gr.imsi.athenarc.visual.middleware.datasource.connector;

import java.sql.SQLException;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

public class PostgreSQLConnector implements DatasourceConnector {
    private final JDBCConnection connection;

    public PostgreSQLConnector(JDBCConnection connection) {
        this.connection = (JDBCConnection) connection.connect();
    }

    @Override
    public AbstractDataset initializeDataset(String schema, String id) {
        try {
            return new PostgreSQLDataset(connection, id, schema, id);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public QueryExecutor initializeQueryExecutor(AbstractDataset dataset) {
        PostgreSQLDataset postgresDataset = (PostgreSQLDataset) dataset;
        return connection.getQueryExecutor(postgresDataset);
    }


    @Override
    public void close() {
        try {
            this.connection.closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}