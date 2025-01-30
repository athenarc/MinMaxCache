package gr.imsi.athenarc.visual.middleware.web.rest.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.methods.Method;
import gr.imsi.athenarc.visual.middleware.methods.MethodManager;
import gr.imsi.athenarc.visual.middleware.methods.VisualQuery;
import gr.imsi.athenarc.visual.middleware.methods.VisualQueryResults;
import gr.imsi.athenarc.visual.middleware.datasource.connector.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connector.InfluxDBConnector;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;

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
    public VisualQueryResults performQuery(VisualQuery visualQuery) {
        if (influxDBConnector == null) {
            initializeConnection();
        }

        String schema = visualQuery.getSchema();
        String id = visualQuery.getTable();

        
        // Perform the query asynchronously
        Method method = MethodManager.getOrInitializeMethod(
            visualQuery.getMethodConfig().getKey(),
            schema,
            id,
            influxDBConnector,
            visualQuery.getMethodConfig().getParams()
        );

        return method.executeQuery(visualQuery);
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

