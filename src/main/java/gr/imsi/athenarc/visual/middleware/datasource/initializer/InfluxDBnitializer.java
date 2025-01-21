package gr.imsi.athenarc.visual.middleware.datasource.initializer;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.domain.influxdb.InfluxDBConnection;

public class InfluxDBnitializer implements DatasourceInitializer {
    private final InfluxDBConnection connection;

    public InfluxDBnitializer(InfluxDBConnection connection) {
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
}