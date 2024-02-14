package gr.imsi.athenarc.visual.middleware.domain;

import java.sql.SQLException;


import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;

public interface DatabaseConnection {

    public void connect() throws  SQLException;

    public QueryExecutor getQueryExecutor();

    public QueryExecutor getQueryExecutor(AbstractDataset dataset);

    public void closeConnection() throws SQLException;
    String getType();
}