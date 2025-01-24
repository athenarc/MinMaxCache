package gr.imsi.athenarc.visual.middleware.datasource.connector;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

/* Handles the connection to the datasource. 
 * The connector is responsible for initializing a dataset and a query executor for the dataset.
 */
public interface DatasourceConnector {
    
    AbstractDataset initializeDataset(String schema, String id);
    QueryExecutor initializeQueryExecutor(AbstractDataset dataset);
    void close();
}