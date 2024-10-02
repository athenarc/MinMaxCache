package gr.imsi.athenarc.visual.middleware.web.rest.service;

import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.domain.PostgreSQL.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.web.rest.repository.DatasetRepository;

@Service
public class PostgreSQLService {
     @Value("${postgres.url}")
    private String postgresUrl;

    @Value("${postgres.username}")
    private String postgresUsername;

    @Value("${postgres.password}")
    private String postgresPassword;

    private JDBCConnection jdbcConnection;

    private DatasetRepository datasetRepository;

    // Method to initialize connection manually
    public void initializeConnection() throws SQLException {
        jdbcConnection = new JDBCConnection(postgresUrl, postgresUsername, postgresPassword);
        System.out.println("PostgreSQL connection established.");
    }

    // Sample method to query the database
    public void performQuery(Query query, String schema, String id) throws SQLException {
        if (jdbcConnection == null) {
            initializeConnection();
        }
        PostgreSQLDataset dataset = null;
        if (datasetRepository.existsById(id)) {
            // If it exists, return the dataset
            dataset = (PostgreSQLDataset) datasetRepository.findById(id).orElseThrow(() -> new RuntimeException("Dataset not found."));
        }
        // Initialize the dataset
        dataset = initializeDataset(schema, id);
        // Save the initialized dataset to the repository
        datasetRepository.save(dataset);
        SQLQueryExecutor sqlQueryExecutor = jdbcConnection.getQueryExecutor(dataset);
        MinMaxCache minMaxCache = new MinMaxCache(sqlQueryExecutor, dataset, 0.5, 4, 4);
        minMaxCache.executeQuery(query);
    }

    // Close connection method (optional)
    public void closeConnection() throws SQLException {
        if (jdbcConnection != null && !jdbcConnection.isClosed()) {
            jdbcConnection.closeConnection();;
            System.out.println("PostgreSQL connection closed.");
        }
    }

    private PostgreSQLDataset initializeDataset(String schema, String id){
        try {
            PostgreSQLDataset dataset = new PostgreSQLDataset(jdbcConnection, id, schema, id);
            return dataset;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
