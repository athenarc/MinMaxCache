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
import gr.imsi.athenarc.visual.middleware.cache.MinMaxCacheBuilder;
import gr.imsi.athenarc.visual.middleware.datasource.connector.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connector.PostgreSQLConnector;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.domain.query.Query;

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

    private PostgreSQLConnector postgreSQLConnector;


    // Map to hold the minmaxcache of each dataset
    private final ConcurrentHashMap<String, MinMaxCache> cacheMap = new ConcurrentHashMap<>();

    @Autowired
    public PostgreSQLService() {}


    // Method to initialize connection manually
    public void initializeConnection() throws SQLException {
        JDBCConnection jdbcConnection = (JDBCConnection) new JDBCConnection(postgresUrl, postgresUsername, postgresPassword).connect();
        postgreSQLConnector = new PostgreSQLConnector(jdbcConnection);
        LOG.info("PostgreSQL connection established.");
    }

    public CompletableFuture<QueryResults> performQuery(Query query, String schema, String id) throws SQLException {
        if (postgreSQLConnector == null) {
            initializeConnection();
        }

        // Cancel previous request for this dataset, if any
        CompletableFuture<?> previousRequest = ongoingRequests.put(id, new CompletableFuture<>());
        if (previousRequest != null && !previousRequest.isDone()) {
            previousRequest.cancel(true);
        }

        CompletableFuture<QueryResults> queryFuture = CompletableFuture.supplyAsync(() -> {
            // Execute the query asynchronously
            MinMaxCache minMaxCache = cacheMap.computeIfAbsent(id, key -> {
                return new MinMaxCacheBuilder()
                    .setDatasourceConnector(postgreSQLConnector)
                    .setSchema(schema)
                    .setId(id)
                    .setPrefetchingFactor(0.5)
                    .setAggFactor(4)
                    .setDataReductionRatio(2)
                    .build();

            });

            return minMaxCache.executeQuery(query);
        }).orTimeout(30, TimeUnit.SECONDS); // Timeout after 30 seconds

        ongoingRequests.put(id, queryFuture);
        return queryFuture;
    }

    // Close connection method (optional)
    public void closeConnection() throws SQLException {
        if (postgreSQLConnector != null) {
            postgreSQLConnector.close();
        }
    }
  
    public PostgreSQLDataset getDatasetById(String schema, String id) throws SQLException {
        if (postgreSQLConnector == null) {
            initializeConnection();
        }
        return (PostgreSQLDataset) postgreSQLConnector.initializeDataset(schema, id);
    }
}
