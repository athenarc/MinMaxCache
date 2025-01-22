package gr.imsi.athenarc.visual.middleware.datasource.connector;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class InfluxDBConnection implements DatabaseConnection {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBConnection.class);
    private String config;
    private InfluxDBClient client;
    private String token;
    private String org;
    private String url;
    private String bucket;
    private Properties properties  = new Properties();;

    public InfluxDBConnection(String influxDBCfg) {
        this.config = influxDBCfg;
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(config);
        try {
            properties.load(inputStream);
            token = properties.getProperty("token");
            org = properties.getProperty("org");
            url = properties.getProperty("url");
            bucket = properties.getProperty("bucket");
        }
        catch (Exception e) {
            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public InfluxDBConnection(String url, String org, String token, String bucket) {
        this.url = url;
        this.org = org;
        this.token = token;
        this.bucket = bucket;
    }

    @Override
    public DatabaseConnection connect() {
        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder()
                .readTimeout(10, TimeUnit.MINUTES)
                .writeTimeout(30, TimeUnit.MINUTES)
                .connectTimeout(1, TimeUnit.MINUTES);
        InfluxDBClientOptions options = InfluxDBClientOptions
                .builder()
                .url(url)
                .org(org)
                .bucket(bucket)
                .authenticateToken(token.toCharArray())
                .okHttpClient(okHttpClient)
                .build();
        client = InfluxDBClientFactory.create(options);
        LOG.info("Initialized InfluxDB connection");
        return this;
    }

    private InfluxDBQueryExecutor createQueryExecutor(AbstractDataset dataset) {
        if(client == null){
            LOG.error("Connection is not initialized");
            return null;
        }
        return new InfluxDBQueryExecutor(client, dataset, org);
    }

    @Override
    public InfluxDBQueryExecutor getQueryExecutor(AbstractDataset dataset) {
        return this.createQueryExecutor(dataset);
    }

    @Override
    public void closeConnection() {
        try {
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getClass().getName() + ": " + e.getMessage());
            throw e;
        }
    }
}