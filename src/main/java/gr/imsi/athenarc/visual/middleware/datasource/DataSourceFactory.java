package gr.imsi.athenarc.visual.middleware.datasource;

import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.ModelarDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.*;

public class DataSourceFactory {

    public static DataSource getDataSource(QueryExecutor queryExecutor, AbstractDataset dataset) {
       if(dataset instanceof PostgreSQLDataset)
            return new PostgreSQLDatasource((SQLQueryExecutor) queryExecutor, (PostgreSQLDataset) dataset);
        else if(dataset instanceof InfluxDBDataset)
            return new InfluxDBDatasource((InfluxDBQueryExecutor) queryExecutor, (InfluxDBDataset) dataset);
        else if(dataset instanceof ModelarDBDataset)
            return new ModelarDBDatasource((ModelarDBQueryExecutor) queryExecutor, (ModelarDBDataset) dataset);
        throw new IllegalArgumentException("Unsupported Datasource");
    }
}
