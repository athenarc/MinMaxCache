package gr.imsi.athenarc.visual.middleware.datasource.connector;

import java.sql.SQLException;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;

abstract interface DatabaseConnection {

    public DatabaseConnection connect() throws  SQLException;
    
    public QueryExecutor getQueryExecutor(AbstractDataset dataset);

    public void closeConnection() throws SQLException;
    
}