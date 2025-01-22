package gr.imsi.athenarc.visual.middleware.datasource.connector;

import java.sql.SQLException;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

abstract interface DatabaseConnection {

    public DatabaseConnection connect() throws  SQLException;
    
    public QueryExecutor getQueryExecutor(AbstractDataset dataset);

    public void closeConnection() throws SQLException;
    
}