package gr.imsi.athenarc.visual.middleware.datasource.executor;

import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.visual.middleware.datasource.connection.DatabaseConnection;
import gr.imsi.athenarc.visual.middleware.datasource.connection.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.datasource.query.InfluxDBQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InfluxDBQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBQueryExecutor.class);

    private final InfluxDBConnection databaseConnection;

    public InfluxDBQueryExecutor(DatabaseConnection databaseConnection) {
        this.databaseConnection = (InfluxDBConnection) databaseConnection;
    }

    public List<FluxTable> executeM4InfluxQuery(InfluxDBQuery q) {
        String flux = q.m4QuerySkeleton();
        return executeDbQuery(flux);
    }


    public List<FluxTable> executeMinMaxInfluxQuery(InfluxDBQuery q) {
        String flux = q.minMaxQuerySkeleton();
        return executeDbQuery(flux);
    }


    public List<FluxTable> executeRawInfluxQuery(InfluxDBQuery q){
        String flux = q.rawQuerySkeleton();
        return executeDbQuery(flux);
    }


    public List<FluxTable> executeDbQuery(String query) {
        QueryApi queryApi = databaseConnection.getClient().getQueryApi();
        LOG.info("Executing Query: \n" + query);
        return queryApi.query(query);
    }

    public void closeConnection() {
        databaseConnection.closeConnection();;
    }

}