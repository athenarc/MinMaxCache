package gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor;

import gr.imsi.athenarc.visual.middleware.datasource.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.SQLQuery;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.TableInfo;
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
                    new ImmutableDataPoint(epoch, val));
            data.computeIfAbsent(measure, m -> new ArrayList<>()).add(
                    new ImmutableDataPoint(epoch2, val));
        }
        data.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        queryResults.setData(data);
        return queryResults;
    }

    public ResultSet execute(String query) throws SQLException {
        LOG.info("Executing Query: \n" + query);
        PreparedStatement preparedStatement =  connection.prepareStatement(query);
        return preparedStatement.executeQuery();
    }

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public List<TableInfo> getTableInfo() throws SQLException {
        DatabaseMetaData databaseMetaData = null;
        List<TableInfo> tableInfoArray = new ArrayList<TableInfo>();
        try {
            databaseMetaData = connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"});
            while(resultSet.next()) {
                TableInfo tableInfo = new TableInfo();
                String tableName = resultSet.getString("TABLE_NAME");
                String schemaName = resultSet.getString("TABLE_SCHEM");
                tableInfo.setSchema(schemaName);
                tableInfo.setTable(tableName);
                tableInfoArray.add(tableInfo);
            }
            return tableInfoArray;
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public List<String> getColumns(String tableName) throws SQLException {
        DatabaseMetaData databaseMetaData = null;
        List<String> columns = new ArrayList<String>();
        try {
            databaseMetaData = connection.getMetaData();
            ResultSet resultSet = databaseMetaData.getColumns(null, null, tableName, null);
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                columns.add(columnName);
            }
            return columns;
        } catch(Exception e) {
            throw e;
        }

    }
    @Override
    public List<Object[]> getSample(String schema, String tableName) throws SQLException {
        String query = "SELECT * FROM " + schema + "." + tableName + " LIMIT 10;";
        List<Object[]> resultList = new ArrayList<>();
        ResultSet resultSet = this.execute(query);
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        String[] columnNames = new String[columnCount];

        for (int i = 1; i <= columnCount; i++) {
            columnNames[i - 1] = metaData.getColumnName(i);
        }

        while (resultSet.next()) {
            Object[] row = new Object[columnCount];

            for (int i = 1; i <= columnCount; i++) {
                row[i - 1] = resultSet.getObject(i);
            }

            resultList.add(row);
        }

        return resultList;

    }
}

