package gr.imsi.athenarc.visual.middleware.datasource.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnection implements DatabaseConnection {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCConnection.class);

    String host;
    String user;
    String password;
    Connection connection;

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

    public String getHost() {
        return host;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public Connection getConnection() {
        return connection;
    }

    

}
