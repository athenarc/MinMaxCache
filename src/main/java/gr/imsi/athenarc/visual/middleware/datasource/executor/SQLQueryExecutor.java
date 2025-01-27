package gr.imsi.athenarc.visual.middleware.datasource.executor;

import gr.imsi.athenarc.visual.middleware.cache.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.SQLQuery;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.*;

import java.util.*;
import java.util.stream.Collectors;

public class SQLQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SQLQueryExecutor.class);

    PostgreSQLDataset dataset;
    Connection connection;
    String table;
    String schema;
    private final String dropFolder = "postgres-drop-queries";
    private final String initFolder = "postgres-init-queries";


    public SQLQueryExecutor(Connection connection) {
        this.connection = connection;
    }
    public SQLQueryExecutor(Connection connection, AbstractDataset dataset) {
        this.connection = connection;
        this.dataset = (PostgreSQLDataset) dataset;
        this.schema = dataset.getSchema();
        this.table = dataset.getTableName();
    }

    @Override
    public QueryResults execute(DataSourceQuery q, QueryMethod method) throws SQLException {
        switch (method) {
            case M4:
                return executeM4Query(q);
            case RAW:
                return executeRawQuery(q);
            case MIN_MAX:
                return executeMinMaxQuery(q);
            default:
                throw new UnsupportedOperationException("Unsupported Query Method");
        }
    }

    @Override
    public QueryResults executeM4Query(DataSourceQuery q) throws SQLException {
        return collect(executeM4SqlQuery((SQLQuery) q));
    }

    @Override
    public QueryResults executeRawQuery(DataSourceQuery q) throws SQLException {
        return collect(executeRawSqlQuery((SQLQuery) q));
    }

    @Override
    public QueryResults executeMinMaxQuery(DataSourceQuery q) throws SQLException {
        return collect(executeMinMaxSqlQuery((SQLQuery) q));
    }

    @Override
    public void initialize(String path) throws SQLException {
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(initFolder + "/" + table + ".sql");
        String[] statements = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n")).split(";");
        for (String statement : statements){
            LOG.info("Executing: " + statement);
            connection.prepareStatement(statement.replace("%path", path)).executeUpdate();
        }
    }

    @Override
    public void drop() throws SQLException {
        String name = Paths.get(dropFolder, table + ".sql").toString();
        LOG.info("Opening {}", name);
        InputStream inputStream
                = getClass().getClassLoader().getResourceAsStream(name);
        String[] statements = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n")).split(";");
        for (String statement : statements){
            LOG.info("Executing: " + statement);
            connection.prepareStatement(statement).executeUpdate();
        }
    }

    Comparator<DataPoint> compareLists = new Comparator<DataPoint>() {
        @Override
        public int compare(DataPoint s1, DataPoint s2) {
            if (s1==null && s2==null) return 0; //swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };


    public ResultSet executeRawSqlQuery(SQLQuery q) throws SQLException{
        String query = q.rawQuerySkeleton();
        return execute(query);
    }


    public ResultSet executeM4SqlQuery(SQLQuery q) throws SQLException {
        String query = q.m4QuerySkeleton();
        return execute(query);
    }


    public ResultSet executeMinMaxSqlQuery(SQLQuery q) throws SQLException {
        String query = q.minMaxQuerySkeleton();
        return execute(query);
    }


    private QueryResults collect(ResultSet resultSet) throws SQLException {
        QueryResults queryResults = new QueryResults();
        HashMap<Integer, List<DataPoint>> data = new HashMap<>();
        while(resultSet.next()){
            Integer measure = Arrays.asList(dataset.getHeader()).indexOf(resultSet.getString(1)); // measure
            long epoch = resultSet.getLong(2); // min_timestamp
            long epoch2 = resultSet.getLong(3); // max_timestamp
            Double val = resultSet.getObject(4) == null ? null : resultSet.getDouble(4); // value
            if(val == null) continue;
            data.computeIfAbsent(measure, m -> new ArrayList<>()).add(
                    new ImmutableDataPoint(epoch, val, measure));
            data.computeIfAbsent(measure, m -> new ArrayList<>()).add(
                    new ImmutableDataPoint(epoch2, val, measure));
        }
        data.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        queryResults.setData(data);
        return queryResults;
    }

    public ResultSet execute(String query) throws SQLException {
        LOG.debug("Executing Query: \n" + query);
        PreparedStatement preparedStatement =  connection.prepareStatement(query);
        return preparedStatement.executeQuery();
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }
}

