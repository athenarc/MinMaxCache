package gr.imsi.athenarc.visual.middleware.datasource.connector;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

/*  */
public interface DatasourceConnector {
    
    AbstractDataset initializeDataset(String schema, String id);
    QueryExecutor initializeQueryExecutor(AbstractDataset dataset);
    void close();
}