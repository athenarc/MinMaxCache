package gr.imsi.athenarc.visual.middleware.domain;

import java.sql.SQLException;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;

public interface DatabaseConnection {

    public DatabaseConnection connect() throws  SQLException;

    public QueryExecutor getQueryExecutor();

    public QueryExecutor getQueryExecutor(AbstractDataset dataset);

    public void closeConnection() throws SQLException;
    
    String getType();
}