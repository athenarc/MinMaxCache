package gr.imsi.athenarc.visual.middleware.web.rest.service;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

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

    // Sample method to query the database
    public QueryResults performQuery(Query query, String schema, String id) throws SQLException {
        if (jdbcConnection == null) {
            initializeConnection();
        }

        PostgreSQLDataset dataset;
        if (datasetRepository.existsById(id)) {
            // If it exists, return the dataset
            dataset = datasetRepository.findById(id).orElseThrow(() -> new RuntimeException("Dataset not found."));
        }
        else {
            // Initialize the dataset
            dataset = initializeDataset(schema, id);
            datasetRepository.save(dataset);
        }
        
        // Check if cache exists in memory, if not, create it
        MinMaxCache minMaxCache = cacheMap.computeIfAbsent(id, key -> {
            SQLQueryExecutor sqlQueryExecutor = jdbcConnection.getQueryExecutor(dataset);
            return new MinMaxCache(sqlQueryExecutor, dataset, 0.5, 4, 4);
        });

        return minMaxCache.executeQuery(query);
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
}
