package gr.imsi.athenarc.visual.middleware.datasource.executor;

import gr.imsi.athenarc.visual.middleware.datasource.connection.DatabaseConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connection.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.datasource.query.SQLQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;


public class SQLQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);

    JDBCConnection connection;

    public SQLQueryExecutor(DatabaseConnection connection) {
        this.connection = (JDBCConnection) connection;
    }

    public ResultSet executeRawSqlQuery(SQLQuery q) throws SQLException{
        String query = q.rawQuerySkeleton();
        return executeDbQuery(query);
    }


    public ResultSet executeM4SqlQuery(SQLQuery q) throws SQLException {
        String query = q.m4QuerySkeleton();
        return executeDbQuery(query);
    }


    public ResultSet executeMinMaxSqlQuery(SQLQuery q) throws SQLException {
        String query = q.minMaxQuerySkeleton();
        return executeDbQuery(query);
    }

    public ResultSet executeDbQuery(String query) throws SQLException {
        LOG.debug("Executing Query: \n" + query);
        PreparedStatement preparedStatement =  connection.getConnection().prepareStatement(query);
        return preparedStatement.executeQuery();
    }

    public void closeConnection(){
        try {
            connection.closeConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

