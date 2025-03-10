package gr.imsi.athenarc.visual.middleware.datasource;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.visual.middleware.datasource.config.*;
import gr.imsi.athenarc.visual.middleware.datasource.connection.*;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.*;
import gr.imsi.athenarc.visual.middleware.datasource.executor.*;
import gr.imsi.athenarc.visual.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

public class DataSourceFactory {
    
    public static DataSource createDataSource(DataSourceConfiguration config) {
        if (config instanceof InfluxDBConfiguration) {
            return createInfluxDBDataSource((InfluxDBConfiguration) config);
        }
        else if (config instanceof PostgreSQLConfiguration) {
            return createPostgreSQLDataSource((PostgreSQLConfiguration) config);
        } else if (config instanceof CsvConfiguration) {
            return createCsvDataSource((CsvConfiguration) config);
        }
        throw new IllegalArgumentException("Unsupported data source configuration");
    }

    private static DataSource createInfluxDBDataSource(InfluxDBConfiguration config) {
        InfluxDBConnection connection = (InfluxDBConnection) new InfluxDBConnection(
            config.getUrl(), 
            config.getOrg(), 
            config.getToken(), 
            config.getBucket()
        ).connect();
        InfluxDBQueryExecutor executor = new InfluxDBQueryExecutor(connection);

        InfluxDBDataset dataset = new InfluxDBDataset(
            config.getMeasurement(),
            config.getBucket(),
            config.getMeasurement()
        );

        fillInfluxDBDatasetInfo(dataset, executor);

        return new InfluxDBDatasource(executor, dataset);
    }

    private static DataSource createPostgreSQLDataSource(PostgreSQLConfiguration config) {
        PostgreSQLDataset dataset;
        try {
            JDBCConnection connection = (JDBCConnection) new JDBCConnection(
            config.getUrl(), 
            config.getUsername(), 
            config.getPassword()
            ).connect();
            SQLQueryExecutor executor = new SQLQueryExecutor(connection);

            dataset = new PostgreSQLDataset(
                config.getTable(),
                config.getSchema(),
                config.getTable()
            );
            fillPostgreSQLDatasetInfo(dataset, executor);
            return new PostgreSQLDatasource(executor, dataset);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static DataSource createCsvDataSource(CsvConfiguration config) {
        try {
            CsvDataset dataset = new CsvDataset(config.getPath(), config.getTimeFormat(), config.getTimeCol(), config.getDelimiter(), config.getHasHeader());
            QueryExecutor executor = new CsvQueryExecutor(dataset);
            return new CsvDatasource((CsvQueryExecutor) executor, (CsvDataset) dataset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void fillInfluxDBDatasetInfo(InfluxDBDataset dataset, InfluxDBQueryExecutor influxDBQueryExecutor) {
        // Fetch first timestamp
        String firstQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start: 1970-01-01T00:00:00.000Z, stop: now())\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> first()\n";
    
        List<FluxTable> fluxTables = influxDBQueryExecutor.executeDbQuery(firstQuery);
        FluxRecord firstRecord = fluxTables.get(0).getRecords().get(0);
        long firstTime = firstRecord.getTime().toEpochMilli();
    
        // Fetch last timestamp
        String lastQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start: 1970-01-01T00:00:00.000Z, stop: now())\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> last()\n";
    
        fluxTables = influxDBQueryExecutor.executeDbQuery(lastQuery);
        FluxRecord lastRecord = fluxTables.get(0).getRecords().get(0);
        long lastTime = lastRecord.getTime().toEpochMilli();
    
        String influxFormat = "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"";
        // Fetch the second timestamp to calculate the sampling interval.
        // Query on first time plus some time later
        String secondQuery = "from(bucket:\"" + dataset.getSchema() + "\")\n" +
            "  |> range(start:" + DateTimeUtil.format(firstTime, influxFormat).replace("\"", "") + 
            ", stop: " + DateTimeUtil.format(firstTime + 360000, influxFormat).replace("\"", "") + ")\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + dataset.getTableName() + "\")\n" +
            "  |> limit(n: 2)\n";  // Fetch the first two records
    
        fluxTables = influxDBQueryExecutor.executeDbQuery(secondQuery);
        FluxRecord secondRecord = fluxTables.get(0).getRecords().get(1); 
        long secondTime = secondRecord.getTime().toEpochMilli();
    
        // Calculate and set sampling interval
        dataset.setSamplingInterval(secondTime - firstTime);
    
        // Set time range and headers
        dataset.setTimeRange(new TimeRange(firstTime, lastTime));
    
        // Populate header (field keys)
        Set<String> header = fluxTables.stream()
            .flatMap(fluxTable -> fluxTable.getRecords().stream())
            .map(FluxRecord::getField)
            .collect(Collectors.toSet());
        
        dataset.setHeader(header.toArray(new String[0]));
    }

    private static void fillPostgreSQLDatasetInfo(PostgreSQLDataset dataset, SQLQueryExecutor sqlQueryExecutor) throws SQLException {
        // Use information_schema to retrieve metadata about the table's columns
        String columnInfoQuery = "SELECT column_name, data_type FROM information_schema.columns " +
                                 "WHERE table_schema = '" + dataset.getSchema() + "' AND table_name = '" + dataset.getTableName() + "';";

        ResultSet resultSet = sqlQueryExecutor.executeDbQuery(columnInfoQuery);
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
        if (!potentialTimeCols.isEmpty()) dataset.setTimeCol(potentialTimeCols.get(0));
        if (!potentialIdCols.isEmpty()) dataset.setIdCol(potentialIdCols.get(0));
        if (!potentialValueCols.isEmpty()) dataset.setValueCol(potentialValueCols.get(0));

        // Now that we have the columns, continue with fetching the metadata
        fillMetadata(dataset, sqlQueryExecutor);
    }

    private static void fillMetadata(PostgreSQLDataset dataset, SQLQueryExecutor sqlQueryExecutor) throws SQLException {
        ResultSet resultSet;

        // Header query to fetch distinct measures
        String headerQuery = "SELECT DISTINCT(" + dataset.getIdCol() + ") FROM " + dataset.getSchema() + "." + dataset.getTableName() + " " +
                             "ORDER BY " + dataset.getIdCol() + " ASC";
        resultSet = sqlQueryExecutor.executeDbQuery(headerQuery);
        List<String> header = new ArrayList<>();
        while (resultSet.next()) {
            header.add(resultSet.getString(1));
        }
        dataset.setHeader(header.toArray(new String[0]));

        // Query for the first and second timestamps to determine sampling interval
        String firstQuery = "SELECT EXTRACT(epoch FROM " + dataset.getTimeCol() + ") * 1000 " +
                            "FROM " + dataset.getSchema() + "." + dataset.getTableName() + " " +
                            "WHERE " + dataset.getIdCol() + " = '" + header.get(dataset.getMeasures().get(0)) + "' " +
                            "ORDER BY " + dataset.getTimeCol() + " ASC " +
                            "LIMIT 2;";
        resultSet = sqlQueryExecutor.executeDbQuery(firstQuery);
        resultSet.next();
        long from = resultSet.getLong(1);
        resultSet.next();
        long second = resultSet.getLong(1);
        dataset.setSamplingInterval(second - from);

        // Query for the last timestamp
        String lastQuery = "SELECT EXTRACT(epoch FROM " + dataset.getTimeCol() + ") * 1000 " +
                           "FROM " + dataset.getSchema() + "." + dataset.getTableName() + " " +
                           "ORDER BY " + dataset.getTimeCol() + " DESC " +
                           "LIMIT 1;";
        resultSet = sqlQueryExecutor.executeDbQuery(lastQuery);
        resultSet.next();
        long to = resultSet.getLong(1);
        dataset.setTimeRange(new TimeRange(from, to));
    }

}
