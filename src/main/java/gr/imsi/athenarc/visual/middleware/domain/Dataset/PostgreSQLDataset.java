package gr.imsi.athenarc.visual.middleware.domain.Dataset;

import gr.imsi.athenarc.visual.middleware.domain.PostgreSQL.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PostgreSQLDataset extends AbstractDataset {

    private String config;

    public PostgreSQLDataset(SQLQueryExecutor sqlQueryExecutor, String id, String schema, String table,
                             String timeFormat, String timeCol, String idCol, String valueCol) throws SQLException {
        super(id, table, schema, timeFormat, timeCol, idCol, valueCol);
        setTimeCol(timeCol);
        this.fillPostgreSQLDatasetInfo(sqlQueryExecutor);
    }


    public PostgreSQLDataset(String config, String id, String schema, String table,
                             String timeFormat, String timeCol, String idCol, String valueCol) throws SQLException {
        super(id, table, schema, timeFormat, timeCol, idCol, valueCol);
        this.config = config;
        setTimeCol(timeCol);
        JDBCConnection jdbcConnection = new JDBCConnection(config);
        jdbcConnection.connect();
        this.fillPostgreSQLDatasetInfo(jdbcConnection.getQueryExecutor());
    }

    public PostgreSQLDataset(String config, String id, String schema, String table, String timeFormat) {
        super(id, table, schema, timeFormat);
        this.config = config;
    }

    private void fillPostgreSQLDatasetInfo(SQLQueryExecutor sqlQueryExecutor) throws SQLException {
        ResultSet resultSet;
        // Header query
        String headerQuery = "SELECT DISTINCT(" + getIdCol() + ") FROM " + getSchema() + "." + getTable() + " \n" +
                "ORDER BY " + getIdCol() + " ASC";

        resultSet = sqlQueryExecutor.execute(headerQuery);
        List<String> header = new ArrayList<>();
        while(resultSet.next()) {
            header.add(resultSet.getString(1));
        }
        setHeader(header.toArray(new String[0]));

        // First date and sampling frequency query
        String firstQuery = "SELECT EXTRACT(epoch FROM " + getTimeCol() + ") * 1000 \n" +
                "FROM " + getSchema()  + "." + getTable() + " \n" +
                "WHERE " + getIdCol() + " = " + "'" + header.get(getMeasures().get(0)) + "'" +  " \n" +
                "ORDER BY " + getTimeCol() + " ASC \n" +
                "LIMIT 2;";
        resultSet = sqlQueryExecutor.execute(firstQuery);
        resultSet.next();
        long from = resultSet.getLong(1);
        resultSet.next();
        long second = resultSet.getLong(1);

        setSamplingInterval(Duration.of(second - from, ChronoUnit.MILLIS));
        // Last date query
        String lastQuery = "SELECT EXTRACT(epoch FROM " + getTimeCol() + ") * 1000 \n" +
                "FROM " + getSchema()  + "." + getTable() + "\n" +
                "ORDER BY " + getTimeCol() + " DESC \n" +
                "LIMIT 1;";
        resultSet = sqlQueryExecutor.execute(lastQuery);
        resultSet.next();
        long to = resultSet.getLong(1);
        setTimeRange(new TimeRange(from, to));
    }


    @Override
    public List<Integer> getMeasures() {
        int[] measures = new int[getHeader().length];
        for(int i = 0; i < measures.length; i++)
            measures[i] = i;
        return Arrays.stream(measures)
                .boxed()
                .collect(Collectors.toList());
    }
    public String getConfig() {
        return config;
    }

}
