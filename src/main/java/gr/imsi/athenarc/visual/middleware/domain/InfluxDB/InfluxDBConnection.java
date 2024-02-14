package gr.imsi.athenarc.visual.middleware.domain.InfluxDB;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.DatabaseConnection;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
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
    public void connect() {
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

    }

    private InfluxDBQueryExecutor createQueryExecutor(AbstractDataset dataset) {
        return new InfluxDBQueryExecutor(client, dataset, org);
    }

    private InfluxDBQueryExecutor createQueryExecutor() {
        return new InfluxDBQueryExecutor(client, bucket, org);
    }

    @Override
    public InfluxDBQueryExecutor getQueryExecutor() {
        return this.createQueryExecutor();
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

    @Override
    public String getType() {
        return "influx";
    }
}