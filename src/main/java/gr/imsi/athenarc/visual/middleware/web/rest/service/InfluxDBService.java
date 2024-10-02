package gr.imsi.athenarc.visual.middleware.web.rest.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.domain.InfluxDB.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;

@Service
public class InfluxDBService {
    
     @Value("${influxdb.url}")
    private String influxDbUrl;

    @Value("${influxdb.token}")
    private String influxDbToken;

    @Value("${influxdb.org}")
    private String influxDbOrg;

    @Value("${influxdb.bucket}")
    private String influxDbBucket;

    private InfluxDBConnection influxDBConnection;

    // Method to initialize InfluxDB connection manually
    public void initializeConnection() {
        influxDBConnection = new InfluxDBConnection(influxDbUrl, influxDbOrg, influxDbToken, influxDbBucket);
        // LOG.info("InfluxDB connection established.");
    }

    // Sample method to query the database
    public void performQuery(Query query) {
        if (influxDBConnection == null) {
            initializeConnection();
        }
    }

    // Close connection method (optional)
    public void closeConnection() {
        if (influxDBConnection != null) {
            influxDBConnection.closeConnection();
            // LOG.info("InfluxDB connection closed.");
        }
    }
}
