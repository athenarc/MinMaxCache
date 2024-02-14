package gr.imsi.athenarc.visual.middleware.cache;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.visual.middleware.datasource.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.InfluxDBQuery;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.SQLQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.SQLQuery;
import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.domain.Query.QueryMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CacheQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);
    private final AbstractDataset dataset;
    private final Map<Integer, Integer> aggFactors;

    private final int initialAggFactor;
    public CacheQueryExecutor(AbstractDataset dataset, int aggFactor) {
        this.dataset = dataset;
        this.aggFactors = new HashMap<>(dataset.getMeasures().size());
        this.initialAggFactor = aggFactor;
        for(int measure : dataset.getMeasures()) aggFactors.put(measure, aggFactor);
    }

    void updateAggFactor(int measure){
        int prevAggFactor = aggFactors.get(measure);
        aggFactors.put(measure, prevAggFactor * 2);
    }

    public QueryResults executeQuery(Query query, CacheManager cacheManager,
                                     DataProcessor dataProcessor, PrefetchManager prefetchManager){
        LOG.info("Executing Visual Query {}", query);
        if(query.getAccuracy() == 1) return executeM4Query(query, dataProcessor.getQueryExecutor());
        long from = query.getFrom();
        long to = query.getTo();
        QueryResults queryResults = new QueryResults();
        ViewPort viewPort = query.getViewPort();

        long pixelColumnInterval = (to - from) / viewPort.getWidth();
        double queryTime = 0;

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        LOG.debug("Pixel column interval: " + pixelColumnInterval + " ms");
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());

        // Initialize Pixel Columns
        Map<Integer, List<PixelColumn>> pixelColumnsPerMeasure = new HashMap<>(measures.size()); // Lists of pixel columns. One list for every measure.
        for (int measure : measures) {
            List<PixelColumn> pixelColumns = new ArrayList<>();
            for (long j = 0; j < viewPort.getWidth(); j++) {
                long pixelFrom = from + (j * pixelColumnInterval);
                long pixelTo = pixelFrom + pixelColumnInterval;
                PixelColumn pixelColumn = new PixelColumn(pixelFrom, pixelTo, viewPort);
                pixelColumns.add(pixelColumn);
            }
            pixelColumnsPerMeasure.put(measure, pixelColumns);
        }

        Map<Integer, List<TimeSeriesSpan>> overlappingSpansPerMeasure = cacheManager.getFromCache(query, pixelColumnInterval);
        LOG.debug("Overlapping intervals per measure {}", overlappingSpansPerMeasure);
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>(measures.size());
        Map<Integer, Double> errorPerMeasure = new HashMap<>(measures.size());

        // For each measure, get the overlapping spans, add them to pixel columns and calculate the error
        // Compute the aggFactor, and if there is an error double it.
        // Finally, add the measure as missing and flag its missing intervals.
        for(int measure : measures){
            // Get overlapping spans
            List<TimeSeriesSpan> overlappingSpans = overlappingSpansPerMeasure.get(measure);

            // Add to pixel columns
            List<PixelColumn> pixelColumns =  pixelColumnsPerMeasure.get(measure);
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, overlappingSpans);

            // Calculate Error
            ErrorCalculator errorCalculator = new ErrorCalculator();
            double errorForMeasure = errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());
            errorPerMeasure.put(measure, errorForMeasure);
            List<TimeInterval> missingIntervalsForMeasure = errorCalculator.getMissingIntervals();

            // Calculate aggFactor
            double coveragePercentages = 0.0;
            double totalAggFactors = 0.0;
            for (TimeSeriesSpan overlappingSpan : overlappingSpans) {
                long size = overlappingSpan.getAggregateInterval(); // ms
                if(size <= dataset.getSamplingInterval().toMillis()) continue; // if raw data continue
                double coveragePercentage = overlappingSpan.percentage(query); // coverage
                int spanAggFactor = (int) ((double) (pixelColumnInterval) / size);
                totalAggFactors += coveragePercentage * spanAggFactor;
                coveragePercentages += coveragePercentage;
            }
            // The missing intervals get a value equal to the initial value
            for(TimeInterval missingInterval : missingIntervalsForMeasure){
                double coveragePercentage = missingInterval.percentage(query); // coverage
                totalAggFactors += coveragePercentage * initialAggFactor;
                coveragePercentages += coveragePercentage;
            }
            int meanWeightAggFactor = coveragePercentages != 0 ? (int) Math.ceil(totalAggFactors / coveragePercentages) : aggFactors.get(measure);
            aggFactors.put(measure, meanWeightAggFactor);

            // Update aggFactor if there is an error
            if(errorCalculator.hasError()){
                updateAggFactor(measure);
                // Initialize ranges and measures to get all errored data.
                missingIntervalsForMeasure = new ArrayList<>();
                missingIntervalsForMeasure.add(new TimeRange(from, to));
            }
            LOG.debug("Getting {} for measure {}", missingIntervalsForMeasure, measure);
            if(missingIntervalsForMeasure.size() > 0){
                missingIntervalsPerMeasure.put(measure, missingIntervalsForMeasure);
            }
        }
        LOG.info("Errors: {}", errorPerMeasure);
        LOG.info("Agg factors: {}", aggFactors);

        // Fetch the missing data from the data source.
        // Give the measures with misses, their intervals and their respective agg factors.
        Map<Integer, List<TimeSeriesSpan>> missingTimeSeriesSpansPerMeasure = missingIntervalsPerMeasure.size() > 0 ?
                dataProcessor.getMissing(from, to, missingIntervalsPerMeasure, aggFactors, viewPort, query.getQueryMethod()) : new HashMap<>(measures.size());

        List<Integer> measuresWithError = new ArrayList<>();
        // For each measure with a miss, add the fetched data points to the pixel columns and recalculate the error.
        for(int measureWithMiss : missingTimeSeriesSpansPerMeasure.keySet()) {
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measureWithMiss);
            List<TimeSeriesSpan> timeSeriesSpans = missingTimeSeriesSpansPerMeasure.get(measureWithMiss);
            // Add to pixel columns
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, timeSeriesSpans);

            // Recalculate error per measure
            ErrorCalculator errorCalculator = new ErrorCalculator();
            double errorForMeasure = errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());
            if (errorCalculator.hasError()) measuresWithError.add(measureWithMiss);
            errorPerMeasure.put(measureWithMiss, errorForMeasure);

            // Add them all to the cache.
            cacheManager.addToCache(timeSeriesSpans);
        }
        LOG.info("Errors: {}", errorPerMeasure);

        // Fetch errored measures with M4
        if(!measuresWithError.isEmpty()) {
            Query m4Query = new Query(from , to, 1.0f, query.getFilter(), QueryMethod.M4, measuresWithError, query.getViewPort(), query.getOpType());
            return executeM4Query(m4Query, dataProcessor.getQueryExecutor());
        }

        // Query Results
        Map<Integer, List<DataPoint>> resultData = new HashMap<>(measures.size());
        Map<Integer, DoubleSummaryStatistics> measureStatsMap = new HashMap<>(measures.size());
        for (int measure : measures) {
            int count = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0;
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measure);
            List<DataPoint> dataPoints = new ArrayList<>();

            for (PixelColumn pixelColumn : pixelColumns) {
                Stats pixelColumnStats = pixelColumn.getStats();
                if (pixelColumnStats.getCount() <= 0) {
                    continue;
                }
                // filter
                if(query.getFilter() == null || query.getFilter().isEmpty()){
                    dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getFirstTimestamp(), pixelColumnStats.getFirstValue()));
                    dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMinTimestamp(), pixelColumnStats.getMinValue()));
                    dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMaxTimestamp(), pixelColumnStats.getMaxValue()));
                    dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getLastTimestamp(), pixelColumnStats.getLastValue()));
                }
                else {
                    double filterMin = query.getFilter().get(measure)[0];
                    double filterMax = query.getFilter().get(measure)[1];
                    if (filterMin < pixelColumnStats.getMinValue() &&
                            filterMax > pixelColumnStats.getMaxValue()) {
                        dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getFirstTimestamp(), pixelColumnStats.getFirstValue()));
                        dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMinTimestamp(), pixelColumnStats.getMinValue()));
                        dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMaxTimestamp(), pixelColumnStats.getMaxValue()));
                        dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getLastTimestamp(), pixelColumnStats.getLastValue()));
                    }
                }
                // compute statistics
                count += 1;
                if(max < pixelColumnStats.getMaxValue()) max = pixelColumnStats.getMaxValue();
                if(min > pixelColumnStats.getMinValue()) min = pixelColumnStats.getMinValue();
                sum += pixelColumnStats.getMaxValue() + pixelColumnStats.getMinValue();
            }
            DoubleSummaryStatistics measureStats = new
                    DoubleSummaryStatistics(count, min, max, sum);
            measureStatsMap.put(measure, measureStats);
            resultData.put(measure, dataPoints);
        }
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();

        // Prefetching
        prefetchManager.prefetch(query, aggFactors);

        resultData.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        queryResults.setData(resultData);
        queryResults.setMeasureStats(measureStatsMap);
        queryResults.setError(errorPerMeasure);
        queryResults.setFlag(measuresWithError.size() > 0);
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(from, to));
        queryResults.setAggFactors(aggFactors);
        return queryResults;
    }

    private QueryResults executeM4Query(Query query, QueryExecutor queryExecutor) {
        QueryResults queryResults = null;
        double queryTime = 0;

        Stopwatch stopwatch = Stopwatch.createStarted();
        QueryMethod queryMethod = QueryMethod.M4;
        DataSourceQuery dataSourceQuery = null;
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<String, List<TimeInterval>> missingTimeIntervalsPerMeasureName = new HashMap<>(query.getMeasures().size());
        Map<String, Integer> numberOfGroupsPerMeasureName = new HashMap<>(query.getMeasures().size());
        Map<Integer, Integer> numberOfGroups = new HashMap<>(query.getMeasures().size());

        for (Integer measure : query.getMeasures()) {
            String measureName = dataset.getHeader()[measure];
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getTo()));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
            missingTimeIntervalsPerMeasureName.put(measureName, timeIntervalsForMeasure);
            numberOfGroups.put(measure, query.getViewPort().getWidth());
            numberOfGroupsPerMeasureName.put(measureName, query.getViewPort().getWidth());
        }
        if(queryExecutor instanceof SQLQueryExecutor)
            dataSourceQuery = new SQLQuery(dataset.getSchema(), dataset.getTable(), dataset.getTimeCol(), dataset.getIdCol(), dataset.getValueCol(),
                    query.getFrom(), query.getTo(), missingTimeIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
        else if (queryExecutor instanceof InfluxDBQueryExecutor)
            dataSourceQuery = new InfluxDBQuery(dataset.getSchema(), dataset.getTable(), query.getFrom(), query.getTo(), missingTimeIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
        else {
            throw new RuntimeException("Unsupported query executor");
        }
        try {
            queryResults = queryExecutor.execute(dataSourceQuery, queryMethod);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        Map<Integer, Double> error = new HashMap<>();
        for(Integer m : query.getMeasures()){
            error.put(m, 0.0);
        }
        queryResults.setError(error);

        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();
        queryResults.setQueryTime(queryTime);

        return queryResults;
    }
}
