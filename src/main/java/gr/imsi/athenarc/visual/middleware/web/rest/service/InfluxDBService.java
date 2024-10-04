package gr.imsi.athenarc.visual.middleware.web.rest.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
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

     @Autowired
    public InfluxDBService(InfluxDBDatasetRepository datasetRepository) {
        this.datasetRepository = datasetRepository;
    }

    // Method to initialize InfluxDB connection manually
    public void initializeConnection() {
        influxDBConnection =  (InfluxDBConnection) new InfluxDBConnection(influxDbUrl, influxDbOrg, influxDbToken, influxDbBucket).connect();
        LOG.info("InfluxDB connection established.");
    }

    // Sample method to query the database
    public void performQuery(Query query, String schema, String id) {
        if (influxDBConnection == null) {
            initializeConnection();
        }

        InfluxDBDataset dataset = null;
        if (datasetRepository.existsById(id)) {
            // If it exists, return the dataset
            dataset = (InfluxDBDataset) datasetRepository.findById(id).orElseThrow(() -> new RuntimeException("Dataset not found."));
        }
        else{
            // Initialize the dataset
            dataset = initializeDataset(schema, id);
            // Save the initialized dataset to the repository
            datasetRepository.save(dataset);
        }
        InfluxDBQueryExecutor influxDBQueryExecutor = influxDBConnection.getQueryExecutor(dataset);
        MinMaxCache minMaxCache = new MinMaxCache(influxDBQueryExecutor, dataset, 0.5, 4, 4);
        minMaxCache.executeQuery(query);
    }

    // Close connection method (optional)
    public void closeConnection() {
        if (influxDBConnection != null) {
            influxDBConnection.closeConnection();
            LOG.info("InfluxDB connection closed.");
        }
    }

    private InfluxDBDataset initializeDataset(String schema, String id){
        InfluxDBDataset dataset = new InfluxDBDataset(influxDBConnection, id, schema, id);
        return dataset;
    }
}
