package gr.imsi.athenarc.visual.middleware.datasource.connector;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.SQLQueryExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCConnection implements DatabaseConnection {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCConnection.class);

    String config;
    String host;
    String user;
    String password;
    Connection connection;
    private final Properties properties = new Properties();

    public JDBCConnection(String config) {
        this.config = config;
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(config);
        try {
            properties.load(inputStream);
            host = properties.getProperty("host");
            user = properties.getProperty("user");
            password = properties.getProperty("password");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JDBCConnection(String host, String user, String password){
        this.host = host;
        this.user = user;
        this.password = password;
    }
    
    @Override
    public DatabaseConnection connect() {
        connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager
                    .getConnection(host, user, password);
            LOG.info("Initialized JDBC connection {}", host);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getClass().getName()+": "+e.getMessage());
        }
        return this;
    }


    @Override
    public void closeConnection() throws SQLException {
        try {
            connection.close();
        } catch (Exception e) {
            LOG.error(e.getClass().getName()+": "+e.getMessage());
            throw e;
        }
    }

    private SQLQueryExecutor createQueryExecutor(AbstractDataset dataset) {
        if(connection == null){
            LOG.error("Connection is not initialized");
            return null;
        }
        return new SQLQueryExecutor(connection, dataset);
    }

    public boolean isClosed(){
        try {
            return connection.isClosed();
        } catch (SQLException e) {
            e.printStackTrace();
            return true;
        }
    }

    @Override
    public SQLQueryExecutor getQueryExecutor(AbstractDataset dataset) {
        return this.createQueryExecutor(dataset);
    }
}
