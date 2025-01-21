package gr.imsi.athenarc.visual.middleware.datasource.connector;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;

public interface DatasourceConnector {
    
    AbstractDataset initializeDataset(String schema, String id);
    QueryExecutor initializeQueryExecutor(AbstractDataset dataset);
    void close();
}