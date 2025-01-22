package gr.imsi.athenarc.visual.middleware.web.rest.service;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.cache.MinMaxCacheBuilder;
import gr.imsi.athenarc.visual.middleware.datasource.connector.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connector.InfluxDBConnector;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.query.Query;

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

    private InfluxDBConnector influxDBConnector;

    // Map to hold the minmaxcache of each dataset
    private final ConcurrentHashMap<String, MinMaxCache> cacheMap = new ConcurrentHashMap<>();

    // Map to track ongoing requests
    private final ConcurrentHashMap<String, CompletableFuture<?>> ongoingRequests = new ConcurrentHashMap<>();

    @Autowired
    public InfluxDBService() {
    }

    // Method to initialize InfluxDB connection manually
    public void initializeConnection() {
        InfluxDBConnection influxDBConnection = (InfluxDBConnection) new InfluxDBConnection(influxDbUrl, influxDbOrg, influxDbToken, influxDbBucket).connect();
        influxDBConnector = new InfluxDBConnector(influxDBConnection);
        LOG.info("InfluxDB connection established.");
    }

    // Method to perform a query with cancellation support
    public CompletableFuture<QueryResults> performQuery(Query query, String schema, String id) {
        if (influxDBConnector == null) {
            initializeConnection();
        }

        // Cancel previous request for this dataset, if any
        CompletableFuture<?> previousRequest = ongoingRequests.put(id, new CompletableFuture<>());
        if (previousRequest != null && !previousRequest.isDone()) {
            previousRequest.cancel(true);
        }

        // Perform the query asynchronously
        CompletableFuture<QueryResults> queryFuture = CompletableFuture.supplyAsync(() -> {
            // Check if cache exists, if not, create it
            MinMaxCache minMaxCache = cacheMap.computeIfAbsent(id, key -> {
               return new MinMaxCacheBuilder()
                    .setDatasourceConnector(influxDBConnector)
                    .setSchema(schema)
                    .setId(id)
                    .setPrefetchingFactor(0.5)
                    .setAggFactor(4)
                    .setDataReductionRatio(2)
                    .build();
            });

            return minMaxCache.executeQuery(query);
        });

        // Track the ongoing request
        ongoingRequests.put(id, queryFuture);
        return queryFuture;
    }

    // Close connection method (optional)
    public void closeConnection() {
        if (influxDBConnector != null) {
            influxDBConnector.close();
            LOG.info("InfluxDB connection closed.");
        }
    }

    public InfluxDBDataset getDatasetById(String schema, String id) {
        if(influxDBConnector == null) {
            initializeConnection();
        }
        return (InfluxDBDataset) influxDBConnector.initializeDataset(schema, id);
    }
}

