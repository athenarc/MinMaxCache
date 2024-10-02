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

    private String timeCol;
    private String idCol;
    private String valueCol;


    // Abstract class implementation
    public PostgreSQLDataset(String id, String schema, String table){
        super(id, schema, table);
    }

    public PostgreSQLDataset(JDBCConnection jdbcConnection, String id, String schema, String table) throws SQLException {
        super(id, schema, table);
        jdbcConnection.connect();
        this.fillPostgreSQLDatasetInfo(jdbcConnection.getQueryExecutor(), schema, table);
    }

    private void fillPostgreSQLDatasetInfo(SQLQueryExecutor sqlQueryExecutor, String schema, String table) throws SQLException {
        // Use information_schema to retrieve metadata about the table's columns
        String columnInfoQuery = "SELECT column_name, data_type FROM information_schema.columns " +
                                 "WHERE table_schema = '" + schema + "' AND table_name = '" + table + "';";

        ResultSet resultSet = sqlQueryExecutor.execute(columnInfoQuery);
        List<String> potentialTimeCols = new ArrayList<>();
        List<String> potentialIdCols = new ArrayList<>();
        List<String> potentialValueCols = new ArrayList<>();

        while (resultSet.next()) {
            String columnName = resultSet.getString("column_name");
            String dataType = resultSet.getString("data_type");

            // Identify potential time column (timestamp data type)
            if (dataType.contains("timestamp")) {
                potentialTimeCols.add(columnName);
            }
            // Identify potential id column (string type)
            else if (dataType.contains("character") || dataType.contains("text")) {
                potentialIdCols.add(columnName);
            }
            // Identify potential value column (numeric or double precision)
            else if (dataType.contains("numeric") || dataType.contains("double precision")) {
                potentialValueCols.add(columnName);
            }
        }

        // Assuming there's only one correct column for each role, or apply rules to select one
        if (!potentialTimeCols.isEmpty()) setTimeCol(potentialTimeCols.get(0));
        if (!potentialIdCols.isEmpty()) setIdCol(potentialIdCols.get(0));
        if (!potentialValueCols.isEmpty()) setValueCol(potentialValueCols.get(0));

        // Now that we have the columns, continue with fetching the metadata
        fillMetadata(sqlQueryExecutor);
    }

    private void fillMetadata(SQLQueryExecutor sqlQueryExecutor) throws SQLException {
        ResultSet resultSet;

        // Header query to fetch distinct measures
        String headerQuery = "SELECT DISTINCT(" + getIdCol() + ") FROM " + getSchema() + "." + getTable() + " " +
                             "ORDER BY " + getIdCol() + " ASC";
        resultSet = sqlQueryExecutor.execute(headerQuery);
        List<String> header = new ArrayList<>();
        while (resultSet.next()) {
            header.add(resultSet.getString(1));
        }
        setHeader(header.toArray(new String[0]));

        // Query for the first and second timestamps to determine sampling interval
        String firstQuery = "SELECT EXTRACT(epoch FROM " + getTimeCol() + ") * 1000 " +
                            "FROM " + getSchema() + "." + getTable() + " " +
                            "WHERE " + getIdCol() + " = '" + header.get(getMeasures().get(0)) + "' " +
                            "ORDER BY " + getTimeCol() + " ASC " +
                            "LIMIT 2;";
        resultSet = sqlQueryExecutor.execute(firstQuery);
        resultSet.next();
        long from = resultSet.getLong(1);
        resultSet.next();
        long second = resultSet.getLong(1);
        setSamplingInterval(Duration.of(second - from, ChronoUnit.MILLIS));

        // Query for the last timestamp
        String lastQuery = "SELECT EXTRACT(epoch FROM " + getTimeCol() + ") * 1000 " +
                           "FROM " + getSchema() + "." + getTable() + " " +
                           "ORDER BY " + getTimeCol() + " DESC " +
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
    
    public String getTimeCol() {
        return timeCol;
    }

    public void setTimeCol(String timeCol) {
        this.timeCol = timeCol;
    }

    public String getIdCol() {
        return idCol;
    }

    public void setIdCol(String idCol) {
        this.idCol = idCol;
    }

    public String getValueCol() {
        return valueCol;
    }

    public void setValueCol(String valueCol) {
        this.valueCol = valueCol;
    }

}
