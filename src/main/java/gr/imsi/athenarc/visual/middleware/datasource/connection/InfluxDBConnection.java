package gr.imsi.athenarc.visual.middleware.datasource.connection;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;

import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class InfluxDBConnection implements DatabaseConnection {


    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBConnection.class);
    private InfluxDBClient client;
    private String token;
    private String org;
    private String url;
    private String bucket;

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

    public InfluxDBClient getClient() {
        return client;
    }

    public String getToken() {
        return token;
    }

    public String getOrg() {
        return org;
    }

    public String getUrl() {
        return url;
    }

    public String getBucket() {
        return bucket;
    }


    
}