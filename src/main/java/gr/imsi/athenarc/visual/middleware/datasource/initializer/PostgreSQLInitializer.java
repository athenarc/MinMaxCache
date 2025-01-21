package gr.imsi.athenarc.visual.middleware.datasource.initializer;

import java.sql.SQLException;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.domain.postgresql.JDBCConnection;

public class PostgreSQLInitializer implements DatasourceInitializer {
    private final JDBCConnection connection;

    public PostgreSQLInitializer(JDBCConnection connection) {
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
}