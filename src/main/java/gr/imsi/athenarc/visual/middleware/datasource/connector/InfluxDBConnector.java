package gr.imsi.athenarc.visual.middleware.datasource.connector;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.dataset.InfluxDBDataset;

public class InfluxDBConnector implements DatasourceConnector {
    private final InfluxDBConnection connection;

    public InfluxDBConnector(InfluxDBConnection connection) {
        this.connection = (InfluxDBConnection) connection.connect();
    }

    @Override
    public AbstractDataset initializeDataset(String schema, String id) {
        return new InfluxDBDataset(connection, id, schema, id);
    }

    @Override
    public QueryExecutor initializeQueryExecutor(AbstractDataset dataset) {
        InfluxDBDataset influxDataset = (InfluxDBDataset) dataset;
        return connection.getQueryExecutor(influxDataset);
    }

    @Override
    public void close() {
        this.connection.closeConnection();
    }
}