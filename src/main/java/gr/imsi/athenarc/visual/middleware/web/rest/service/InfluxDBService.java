package gr.imsi.athenarc.visual.middleware.web.rest.service;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.domain.InfluxDB.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.web.rest.repository.InfluxDBDatasetRepository;

@Service
public class InfluxDBService {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBService.class);

    @Value("${influxdb.url}")
    private String influxDbUrl;

    @Value("${influxdb.token}")
    private String influxDbToken;

    @Value("${influxdb.org}")
    private String influxDbOrg;

    @Value("${influxdb.bucket}")
    private String influxDbBucket;

    private InfluxDBConnection influxDBConnection;

    private final InfluxDBDatasetRepository datasetRepository;

    // Map to hold the minmaxcache of each dataset
    private final ConcurrentHashMap<String, MinMaxCache> cacheMap = new ConcurrentHashMap<>();

    // Map to track ongoing requests
    private final ConcurrentHashMap<String, CompletableFuture<?>> ongoingRequests = new ConcurrentHashMap<>();

    @Autowired
    public InfluxDBService(InfluxDBDatasetRepository datasetRepository) {
        this.datasetRepository = datasetRepository;
    }

    // Method to initialize InfluxDB connection manually
    public void initializeConnection() {
        influxDBConnection = (InfluxDBConnection) new InfluxDBConnection(influxDbUrl, influxDbOrg, influxDbToken, influxDbBucket).connect();
        LOG.info("InfluxDB connection established.");
    }

    // Method to perform a query with cancellation support
    public CompletableFuture<QueryResults> performQuery(Query query, String schema, String id) {
        if (influxDBConnection == null) {
            initializeConnection();
        }

        // Get or create the dataset
        InfluxDBDataset dataset = getDatasetById(schema, id);

        // Cancel previous request for this dataset, if any
        CompletableFuture<?> previousRequest = ongoingRequests.put(id, new CompletableFuture<>());
        if (previousRequest != null && !previousRequest.isDone()) {
            previousRequest.cancel(true);
        }

        // Perform the query asynchronously
        CompletableFuture<QueryResults> queryFuture = CompletableFuture.supplyAsync(() -> {
            // Check if cache exists, if not, create it
            MinMaxCache minMaxCache = cacheMap.computeIfAbsent(id, key -> {
                InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getQueryExecutor(dataset);
                return new MinMaxCache(influxDBQueryExecutor, dataset, 0, 4, 2);
            });

            return minMaxCache.executeQuery(query);
        });

        // Track the ongoing request
        ongoingRequests.put(id, queryFuture);
        return queryFuture;
    }

    // Close connection method (optional)
    public void closeConnection() {
        if (influxDBConnection != null) {
            influxDBConnection.closeConnection();
            LOG.info("InfluxDB connection closed.");
        }
    }

    private InfluxDBDataset initializeDataset(String schema, String id) {
        return new InfluxDBDataset(influxDBConnection, id, schema, id);
    }

    public InfluxDBDataset getDatasetById(String schema, String id) {
        if (influxDBConnection == null) {
            initializeConnection();
        }
        // Check if the dataset already exists
        if (datasetRepository.existsById(id)) {
            return datasetRepository.findById(id)
                    .orElseThrow(() -> new RuntimeException("Dataset not found."));
        } else {
            // Initialize and save a new dataset if it doesn't exist
            LOG.info("Dataset with id {} does not exist. Initializing...", id);
            InfluxDBDataset newDataset = initializeDataset(schema, id); // Use the appropriate schema
            if (newDataset != null) {
                datasetRepository.save(newDataset);
                return newDataset;
            } else {
                throw new RuntimeException("Failed to initialize dataset with id " + id);
            }
        }
    }
}

