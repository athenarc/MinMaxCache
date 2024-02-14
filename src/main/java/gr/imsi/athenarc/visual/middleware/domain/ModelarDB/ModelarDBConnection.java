package gr.imsi.athenarc.visual.middleware.domain.ModelarDB;

import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.ModelarDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.DatabaseConnection;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
import cfjd.org.apache.arrow.flight.FlightClient;
import cfjd.org.apache.arrow.flight.Location;
import cfjd.org.apache.arrow.memory.BufferAllocator;
import cfjd.org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ModelarDBConnection implements Serializable, DatabaseConnection {
    private static final Logger LOG = LoggerFactory.getLogger(ModelarDBConnection.class);

    String config;
    String host;
    int port;
    FlightClient flightClient;
    Connection connection;
    private final Properties properties = new Properties();

    public ModelarDBConnection(String config) {
        this.config = config;
        try {
            InputStream inputStream
                    = getClass().getClassLoader().getResourceAsStream(config);
            properties.load(inputStream);
            this.host = properties.getProperty("host");
            this.port = Integer.parseInt(properties.getProperty("port"));
        }
        catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getClass().getName()+": "+e.getMessage());
        }
        this.connect();
    }

    public ModelarDBConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.connect();
    }

    @Override
    public void connect() {
        connection = null;
        try {
            Location location = Location.forGrpcInsecure(host, port);
            BufferAllocator allocator = new RootAllocator();
            this.flightClient = FlightClient.builder(allocator, location).build();
            LOG.info("Initialized Arrow Flight connection");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getClass().getName()+": "+e.getMessage());
        }
    }


    private ModelarDBQueryExecutor createQueryExecutor(AbstractDataset dataset) {
        return new ModelarDBQueryExecutor(flightClient, dataset);
    }

    private ModelarDBQueryExecutor createQueryExecutor() {
        return new ModelarDBQueryExecutor(flightClient);
    }

    public ModelarDBQueryExecutor getSqlQueryExecutor(AbstractDataset dataset) {
        return this.createQueryExecutor(dataset);
    }

    public ModelarDBQueryExecutor getSqlQueryExecutor() {
        return this.createQueryExecutor();
    }

    @Override
    public QueryExecutor getQueryExecutor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueryExecutor getQueryExecutor(AbstractDataset dataset) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void closeConnection() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getType() {
        return "modelardb";
    }
}
