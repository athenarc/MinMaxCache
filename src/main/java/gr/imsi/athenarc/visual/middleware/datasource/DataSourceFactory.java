package gr.imsi.athenarc.visual.middleware.datasource;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.*;
import gr.imsi.athenarc.visual.middleware.datasource.executor.CsvQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.executor.SQLQueryExecutor;

public class DataSourceFactory {

    public static DataSource getDataSource(QueryExecutor queryExecutor, AbstractDataset dataset) {
        if(dataset instanceof PostgreSQLDataset)
            return new PostgreSQLDatasource((SQLQueryExecutor) queryExecutor, (PostgreSQLDataset) dataset);
        else if(dataset instanceof InfluxDBDataset)
            return new InfluxDBDatasource((InfluxDBQueryExecutor) queryExecutor, (InfluxDBDataset) dataset);
        else if(dataset instanceof CsvDataset)
            return new CsvDatasource((CsvQueryExecutor) queryExecutor, (CsvDataset) dataset);

        throw new IllegalArgumentException("Unsupported Datasource");
    }
}
