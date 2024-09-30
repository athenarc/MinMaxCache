package gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor;

import gr.imsi.athenarc.visual.middleware.domain.Dataset.*;
import gr.imsi.athenarc.visual.middleware.domain.PostgreSQL.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.domain.InfluxDB.InfluxDBConnection;

public class QueryExecutorFactory {

    public static QueryExecutor getQueryExecutor(AbstractDataset dataset) {
        if(dataset instanceof PostgreSQLDataset) {
            JDBCConnection postgreSQLConnection = new JDBCConnection(((PostgreSQLDataset) dataset).getConfig());
            postgreSQLConnection.connect();
            return postgreSQLConnection.getQueryExecutor(dataset);
        }
        else if(dataset instanceof InfluxDBDataset) {
            InfluxDBConnection influxDBConnection = new InfluxDBConnection(((InfluxDBDataset) dataset).getConfig());
            influxDBConnection.connect();
            return influxDBConnection.getQueryExecutor(dataset);
        }
        throw new IllegalArgumentException("Unsupported Datasource");
    }
}
