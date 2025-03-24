package gr.imsi.athenarc.visual.middleware.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import gr.imsi.athenarc.visual.middleware.cache.query.ErrorResults;
import gr.imsi.athenarc.visual.middleware.cache.query.Query;
import gr.imsi.athenarc.visual.middleware.cache.query.QueryResults;
import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoints;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Stats;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import gr.imsi.athenarc.visual.middleware.domain.ViewPort;

public class CacheQueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);
    private final DataSource dataSource;
    private final AbstractDataset dataset;
    private final Map<Integer, Integer> aggFactors;


    private final int initialAggFactor;

    protected CacheQueryExecutor(DataSource dataSource, int aggFactor) {
        this.dataSource = dataSource;
        this.dataset = dataSource.getDataset();
        this.aggFactors = new HashMap<>(dataset.getMeasures().size());
        this.initialAggFactor = aggFactor;
        for(int measure : dataset.getMeasures()) aggFactors.put(measure, aggFactor);
    }

    void updateAggFactor(int measure){
        int prevAggFactor = aggFactors.get(measure);
        aggFactors.put(measure, prevAggFactor * 2);
    }

    protected QueryResults executeQuery(Query query, CacheManager cacheManager,
                                     DataProcessor dataProcessor, PrefetchManager prefetchManager){
        LOG.info("Executing Visual Query {}", query);
        if(query.getAccuracy() == 1) return executeM4Query(query);

        // Bound from and to to dataset range
        long from = Math.max(dataset.getTimeRange().getFrom(), query.getFrom());
        long to = Math.min(dataset.getTimeRange().getTo(), query.getTo());
        QueryResults queryResults = new QueryResults();

        ViewPort viewPort = query.getViewPort();

        long pixelColumnInterval = (to - from) / viewPort.getWidth();
        double queryTime = 0;
        long ioCount = 0;

        Stopwatch stopwatch = Stopwatch.createUnstarted();
        stopwatch.start();
        LOG.debug("Pixel column interval: " + pixelColumnInterval + " ms");
        List<Integer> measures = Optional.ofNullable(query.getMeasures()).orElse(dataset.getMeasures());
        Map<Integer, List<DataPoint>> resultData = new HashMap<>(measures.size());
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
        Map<Integer, ErrorResults> errorPerMeasure = new HashMap<>(measures.size());
        long aggInterval = (query.getTo() - query.getFrom()) / query.getViewPort().getWidth();
       
        // These is where the pixel columns start and end, as the agg interval is not a float.
        long startPixelColumn = from;
        long endPixelColumn = query.getFrom() + aggInterval * (query.getViewPort().getWidth());

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
            ErrorResults errorResults = new ErrorResults();
            double errorForMeasure = errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());
            errorResults.setError(errorForMeasure);
            errorResults.setFalsePixels(errorCalculator.getFalsePixels());
            errorResults.setMissingPixels(errorCalculator.getMissingPixels());
            errorPerMeasure.put(measure, errorResults);
            List<TimeInterval> missingIntervalsForMeasure = errorCalculator.getMissingIntervals();

            // Calculate aggFactor
            double coveragePercentages = 0.0;
            double totalAggFactors = 0.0;
            for (TimeSeriesSpan overlappingSpan : overlappingSpans) {
                long size = overlappingSpan.getAggregateInterval(); // ms
                if(size <= dataset.getSamplingInterval()) continue; // if raw data continue
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
        LOG.debug("Errors: {}", errorPerMeasure);
        LOG.info("Agg factors: {}", aggFactors);

        // Fetch the missing data from the data source.
        // Give the measures with misses, their intervals and their respective agg factors.
        Map<Integer, List<TimeSeriesSpan>> missingTimeSeriesSpansPerMeasure = missingIntervalsPerMeasure.size() > 0 ?
                dataProcessor.getMissing(from, to, missingIntervalsPerMeasure, aggFactors, viewPort) : new HashMap<>(measures.size());

        List<Integer> measuresWithError = new ArrayList<>();
        // For each measure with a miss, add the fetched data points to the pixel columns and recalculate the error.
        for(int measureWithMiss : missingTimeSeriesSpansPerMeasure.keySet()) {
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measureWithMiss);
            List<TimeSeriesSpan> timeSeriesSpans = missingTimeSeriesSpansPerMeasure.get(measureWithMiss);
            ioCount += timeSeriesSpans.stream().mapToLong(TimeSeriesSpan::getCount).sum();
            // Add to pixel columns
            dataProcessor.processDatapoints(from, to, viewPort, pixelColumns, timeSeriesSpans);

            // Recalculate error per measure
            ErrorCalculator errorCalculator = new ErrorCalculator();
            ErrorResults errorResults = new ErrorResults();
            double errorForMeasure = errorCalculator.calculateTotalError(pixelColumns, viewPort, pixelColumnInterval, query.getAccuracy());

            if (errorCalculator.hasError()) measuresWithError.add(measureWithMiss);
            errorResults.setError(errorForMeasure);
            errorResults.setFalsePixels(errorCalculator.getFalsePixels());
            errorResults.setMissingPixels(errorCalculator.getMissingPixels());

            errorPerMeasure.put(measureWithMiss, errorResults);
            pixelColumnsPerMeasure.put(measureWithMiss, pixelColumns);
            // Add them all to the cache.
            cacheManager.addToCache(timeSeriesSpans);
        }
        // Fetch errored measures with M4
        if(!measuresWithError.isEmpty()) {
            Query m4Query = new Query(from, to, measuresWithError, viewPort.getWidth(), viewPort.getHeight(), 1.0f);
            QueryResults m4QueryResults = executeM4Query(m4Query);
            long timeStart = System.currentTimeMillis();
            ioCount += 4 * viewPort.getWidth() * measuresWithError.size();
            measuresWithError.forEach(m -> resultData.put(m, m4QueryResults.getData().get(m))); // add m4 results to final result
            // Set error to 0
            ErrorResults errorResults = new ErrorResults();
            measuresWithError.forEach(m -> errorPerMeasure.put(m, errorResults)); // set error to 0;
            queryResults.setProgressiveQueryTime((System.currentTimeMillis() - timeStart) / 1000F);
        }

        // Query Results
        List<Integer> measuresWithoutError = new ArrayList<>(measures);
        measuresWithoutError.removeAll(measuresWithError); // remove measures handled with m4 query
        Map<Integer, DoubleSummaryStatistics> measureStatsMap = new HashMap<>(measures.size());

        for (int measure : measuresWithoutError) {
            int count = 0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0;
            List<PixelColumn> pixelColumns = pixelColumnsPerMeasure.get(measure);

            List<DataPoint> dataPoints = new ArrayList<>();
            int i = 0;
            for (PixelColumn pixelColumn : pixelColumns) {
                Stats pixelColumnStats = pixelColumn.getStats();
                if (pixelColumnStats.getCount() <= 0) {
                    continue;
                }
                // add points
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getFirstTimestamp(), pixelColumnStats.getFirstValue(), measure));
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMinTimestamp(), pixelColumnStats.getMinValue(), measure));
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getMaxTimestamp(), pixelColumnStats.getMaxValue(), measure));
                dataPoints.add(new ImmutableDataPoint(pixelColumnStats.getLastTimestamp(), pixelColumnStats.getLastValue(), measure));
                
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
        // queryResults.setError(errorPerMeasure);
        queryResults.setQueryTime(queryTime);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryResults.setIoCount(ioCount);
        return queryResults;
    }

    private QueryResults executeM4Query(Query query) {
        QueryResults queryResults = new QueryResults();
        Map<Integer, List<DataPoint>> m4Data = new HashMap<>();
        double queryTime = 0;

        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, List<TimeInterval>> missingΙntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
        Map<Integer, Long> aggregateIntervals = new HashMap<>(query.getMeasures().size());

        Map<String, List<TimeInterval>> missingIntervalsPerMeasureName = new HashMap<>(query.getMeasures().size());
        Map<String, Integer> numberOfGroupsPerMeasureName = new HashMap<>(query.getMeasures().size());
        Map<Integer, Integer> numberOfGroups = new HashMap<>(query.getMeasures().size());
        long aggInterval = (query.getTo() - query.getFrom()) / query.getViewPort().getWidth();

        long startPixelColumn = query.getFrom();
        long endPixelColumn = query.getFrom() + aggInterval * (query.getViewPort().getWidth());


        for (Integer measure : query.getMeasures()) {
            String measureName = dataset.getHeader()[measure];
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getFrom() + aggInterval * (query.getViewPort().getWidth())));
            missingΙntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
            missingIntervalsPerMeasureName.put(measureName, timeIntervalsForMeasure);
            numberOfGroups.put(measure, query.getViewPort().getWidth());
            numberOfGroupsPerMeasureName.put(measureName, query.getViewPort().getWidth());
            aggregateIntervals.put(measure, aggInterval);
        }
        AggregatedDataPoints missingDataPoints = 
            dataSource.getM4DataPoints(startPixelColumn, endPixelColumn, missingΙntervalsPerMeasure, numberOfGroups);
        
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPoints, missingΙntervalsPerMeasure, aggregateIntervals);
        for (Integer measure : query.getMeasures()) {
            List<TimeSeriesSpan> spans = timeSeriesSpans.get(measure);
            List<DataPoint> dataPoints = new ArrayList<>();
            for (TimeSeriesSpan span : spans) {
                Iterator<AggregatedDataPoint> it = ((AggregateTimeSeriesSpan) span).iterator();
                while (it.hasNext()) {
                    AggregatedDataPoint aggregatedDataPoint = it.next();
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getFirstDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getFirstDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMinDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getMinDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMaxDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getMaxDataPoint().getValue(), measure));
                    dataPoints.add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getLastDataPoint().getTimestamp(), aggregatedDataPoint.getStats().getLastDataPoint().getValue(), measure));

                }
            }
            m4Data.put(measure, dataPoints);
        }
        Map<Integer, ErrorResults> error = new HashMap<>();
        for(Integer m : query.getMeasures()){
            error.put(m, new ErrorResults());
        }
        queryResults.setData(m4Data);
        queryResults.setTimeRange(new TimeRange(startPixelColumn, endPixelColumn));
        queryTime = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
        stopwatch.stop();
        queryResults.setQueryTime(queryTime);

        return queryResults;
    }
}
