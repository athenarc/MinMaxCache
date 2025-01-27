package gr.imsi.athenarc.visual.middleware.algorithms;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.visual.middleware.cache.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.PostgreSQLDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.CsvQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.executor.SQLQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.query.CsvQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.InfluxDBQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.SQLQuery;
import gr.imsi.athenarc.visual.middleware.domain.ErrorResults;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import gr.imsi.athenarc.visual.middleware.web.rest.model.VisualQuery;

public class M4Algorithm implements Algorithm {


    Map<String, DatasourceConnector> dataSourceConnnectors = new HashMap<>();
    @Override
    public void initialize(String schema, String datasetId, DatasourceConnector datasourceConnector,
            Map<String, String> params) {
        dataSourceConnnectors.put(datasetId, datasourceConnector);
    }

    @Override
    public QueryResults executeQuery(VisualQuery query) {
        int width = query.getWidth();
        int height = query.getHeight();
        long from = query.getFrom();
        DatasourceConnector datasourceConnector = dataSourceConnnectors.get(query.getTable());
        
        AbstractDataset dataset = datasourceConnector.initializeDataset(query.getSchema(), query.getTable());
        QueryExecutor queryExecutor = datasourceConnector.initializeQueryExecutor(dataset);

        QueryResults queryResults = new QueryResults();
        double queryTime = 0;

        Stopwatch stopwatch = Stopwatch.createStarted();
        DataSourceQuery dataSourceQuery = null;
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<String, List<TimeInterval>> missingTimeIntervalsPerMeasureName = new HashMap<>(query.getMeasures().size());
        Map<String, Integer> numberOfGroupsPerMeasureName = new HashMap<>(query.getMeasures().size());
        Map<Integer, Integer> numberOfGroups = new HashMap<>(query.getMeasures().size());
        long aggInterval = (query.getTo() - query.getFrom()) / width;

        long startPixelColumn = from;
        long endPixelColumn = query.getFrom() + aggInterval * width;

        for (Integer measure : query.getMeasures()) {
            String measureName = dataset.getHeader()[measure];
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getFrom() + aggInterval * width));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
            missingTimeIntervalsPerMeasureName.put(measureName, timeIntervalsForMeasure);
            numberOfGroups.put(measure, width);
            numberOfGroupsPerMeasureName.put(measureName, width);
        }
        if(queryExecutor instanceof SQLQueryExecutor)
            dataSourceQuery = new SQLQuery(dataset.getSchema(), dataset.getTableName(), dataset.getTimeFormat(),
                    ((PostgreSQLDataset)dataset).getTimeCol(), ((PostgreSQLDataset)dataset).getIdCol(), ((PostgreSQLDataset)dataset).getValueCol(),
                    query.getFrom(), query.getTo(), missingTimeIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
        else if (queryExecutor instanceof InfluxDBQueryExecutor)
            dataSourceQuery = new InfluxDBQuery(dataset.getSchema(), dataset.getTableName(), dataset.getTimeFormat(),
             query.getFrom(), query.getTo(), missingTimeIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
        else if (queryExecutor instanceof CsvQueryExecutor)
            dataSourceQuery = new CsvQuery(query.getFrom(), query.getTo(), missingTimeIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
        else {
            throw new RuntimeException("Unsupported query executor");
        }
        try {
            queryResults = queryExecutor.execute(dataSourceQuery, QueryMethod.M4);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<Integer, ErrorResults> error = new HashMap<>();
        for(Integer m : query.getMeasures()){
            error.put(m, new ErrorResults());
        }
        queryResults.setError(error);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();
        queryResults.setQueryTime(queryTime);

        return queryResults;
    }


    public boolean isInitialized(String datasetId) {
        return dataSourceConnnectors.containsKey(datasetId);
    }
    
}
