package gr.imsi.athenarc.visual.middleware.datasource.executor;

import gr.imsi.athenarc.visual.middleware.cache.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

public interface QueryExecutor {

    QueryResults execute(DataSourceQuery q, QueryMethod method) throws IOException, SQLException;
    QueryResults executeM4Query(DataSourceQuery q) throws IOException, SQLException;
    QueryResults executeRawQuery(DataSourceQuery q) throws IOException, SQLException;
    QueryResults executeMinMaxQuery(DataSourceQuery q) throws SQLException, IOException;

    void initialize(String path) throws SQLException, FileNotFoundException, NoSuchMethodException;
    void drop() throws SQLException, FileNotFoundException, NoSuchMethodException;
}
