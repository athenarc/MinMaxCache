package gr.imsi.athenarc.visual.middleware.web.rest.service;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.domain.PostgreSQL.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.web.rest.repository.PostgreSQLDatasetRepository;

@Service
public class PostgreSQLService {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLService.class);

    private final ConcurrentHashMap<String, CompletableFuture<?>> ongoingRequests = new ConcurrentHashMap<>();

    @Value("${postgres.url}")
    private String postgresUrl;

    @Value("${postgres.username}")
    private String postgresUsername;

    @Value("${postgres.password}")
    private String postgresPassword;

    private JDBCConnection jdbcConnection;

    private final PostgreSQLDatasetRepository datasetRepository;

    // Map to hold the minmaxcache of each dataset
    private final ConcurrentHashMap<String, MinMaxCache> cacheMap = new ConcurrentHashMap<>();

    @Autowired
    public PostgreSQLService(PostgreSQLDatasetRepository datasetRepository) {
        this.datasetRepository = datasetRepository;
    }


    // Method to initialize connection manually
    public void initializeConnection() throws SQLException {
        jdbcConnection = (JDBCConnection) new JDBCConnection(postgresUrl, postgresUsername, postgresPassword).connect();
        LOG.info("PostgreSQL connection established.");
    }

    public CompletableFuture<QueryResults> performQuery(Query query, String schema, String id) throws SQLException {
        if (jdbcConnection == null) {
            initializeConnection();
        }

        PostgreSQLDataset dataset = datasetRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Dataset not found."));

        // Cancel previous request for this dataset, if any
        CompletableFuture<?> previousRequest = ongoingRequests.put(id, new CompletableFuture<>());
        if (previousRequest != null && !previousRequest.isDone()) {
            previousRequest.cancel(true);
        }

        CompletableFuture<QueryResults> queryFuture = CompletableFuture.supplyAsync(() -> {
            // Execute the query asynchronously
            MinMaxCache minMaxCache = cacheMap.computeIfAbsent(id, key -> {
                SQLQueryExecutor sqlQueryExecutor = jdbcConnection.getQueryExecutor(dataset);
                return new MinMaxCache(sqlQueryExecutor, dataset, 0.5, 4, 4);
            });

            return minMaxCache.executeQuery(query);
        }).orTimeout(30, TimeUnit.SECONDS); // Timeout after 30 seconds

        ongoingRequests.put(id, queryFuture);
        return queryFuture;
    }

    // Close connection method (optional)
    public void closeConnection() throws SQLException {
        if (jdbcConnection != null && !jdbcConnection.isClosed()) {
            jdbcConnection.closeConnection();;
            // LOG.info("PostgreSQL connection closed.");
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

    public PostgreSQLDataset getDatasetById(String schema, String id) throws SQLException {
        if (jdbcConnection == null) {
            initializeConnection();
        }
        // Check if the dataset already exists
        if (datasetRepository.existsById(id)) {
            return datasetRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("Dataset not found."));
        } else {
            // Initialize and save a new dataset if it doesn't exist
            LOG.info("Dataset with id {} does not exist. Initializing...", id);
            PostgreSQLDataset newDataset = initializeDataset(schema, id); // Use the appropriate schema
            if (newDataset != null) {
                datasetRepository.save(newDataset);
                return newDataset;
            } else {
                throw new RuntimeException("Failed to initialize dataset with id " + id);
            }
        }
    }
}
