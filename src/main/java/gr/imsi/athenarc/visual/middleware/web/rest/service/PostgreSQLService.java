package gr.imsi.athenarc.visual.middleware.web.rest.service;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.datasource.connector.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connector.PostgreSQLConnector;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.methods.VisualQuery;
import gr.imsi.athenarc.visual.middleware.methods.VisualQueryResults;

@Service
public class PostgreSQLService {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLService.class);

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

    public VisualQueryResults performQuery(VisualQuery visualQuery) throws SQLException {
        if (postgreSQLConnector == null) {
            initializeConnection();
        }

        String schema = visualQuery.getSchema();
        String id = visualQuery.getTable();

        return null;
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
