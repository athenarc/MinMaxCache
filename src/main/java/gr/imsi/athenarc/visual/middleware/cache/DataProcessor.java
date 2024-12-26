package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.datasource.DataSourceFactory;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.Query.QueryMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataProcessor {

    private final AbstractDataset dataset;
    private final DataSource dataSource;
    private final int dataReductionRatio;

    private final QueryExecutor queryExecutor;

    public DataProcessor(QueryExecutor queryExecutor, AbstractDataset dataset, int dataReductionRatio){
        this.dataset = dataset;
        this.queryExecutor = queryExecutor;
        this.dataSource = DataSourceFactory.getDataSource(queryExecutor, dataset);
        this.dataReductionRatio = dataReductionRatio;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DataProcessor.class);

    /**
     * Add a list of timeseriesspans to their respective pixel columns.
     * Each span and pixel column list represents a specific measure.
     * @param from start of query
     * @param to end of query
     * @param viewPort viewport of query
     * @param pixelColumns pixel columns of measure
     * @param timeSeriesSpans time series spans for measure
     */
    public void processDatapoints(long from, long to, ViewPort viewPort,
                                   List<PixelColumn> pixelColumns, List<TimeSeriesSpan> timeSeriesSpans) {
        for (TimeSeriesSpan span : timeSeriesSpans) {
            if (span instanceof AggregateTimeSeriesSpan) {
                Iterator<AggregatedDataPoint> iterator = ((AggregateTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    AggregatedDataPoint aggregatedDataPoint = iterator.next();
                    addAggregatedDataPointToPixelColumns(from, to, viewPort, pixelColumns, aggregatedDataPoint);
                }
            }
            else if (span instanceof RawTimeSeriesSpan){
                Iterator<DataPoint> iterator = ((RawTimeSeriesSpan) span).iterator(from, to);
                while (iterator.hasNext()) {
                    DataPoint dataPoint = iterator.next();
                    addDataPointToPixelColumns(from, to, viewPort, pixelColumns, dataPoint);
                }
            }
            else{
                throw new IllegalArgumentException("Time Series Span Read Error");
            }
        }
    }

    protected Map<Integer, List<TimeInterval>> sortMeasuresAndIntervals(Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        // Sort the map by measure alphabetically
        Map<Integer, List<TimeInterval>> sortedMap = new TreeMap<>(Comparator.comparing(Object::toString));
        sortedMap.putAll(missingIntervalsPerMeasure);

        // Sort each list of intervals based on the getFrom() epoch
        for (List<TimeInterval> intervals : sortedMap.values()) {
            if(intervals.size() > 1)
                intervals.sort(Comparator.comparingLong(TimeInterval::getFrom));
        }

        // Update the original map with the sorted values
        missingIntervalsPerMeasure.clear();
        missingIntervalsPerMeasure.putAll(sortedMap);
        return sortedMap;
    }
    /**
     * Get missing data between the range from-to. THe data are fetched for each measure and each measure has a list of missingIntervals as well as
     * an aggregationFactor.
     * @param from start of query
     * @param to end of query
     * @param missingIntervalsPerMeasure missing intervals per measure
     * @param aggFactors aggregation factors per measure
     * @return A list of TimeSeriesSpan for each measure.
     **/
    public Map<Integer, List<TimeSeriesSpan>> getMissing(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                 Map<Integer, Integer> aggFactors, ViewPort viewPort, QueryMethod queryMethod) {
        missingIntervalsPerMeasure = sortMeasuresAndIntervals(missingIntervalsPerMeasure); // This helps with parsing the query results
        Map<Integer, List<TimeSeriesSpan>> timeSeriesSpans = new HashMap<>(missingIntervalsPerMeasure.size());
        Map<Integer, Integer> numberOfGroups = new HashMap<>(missingIntervalsPerMeasure.size());
        Map<Integer, Long> aggregateIntervals = new HashMap<>(missingIntervalsPerMeasure.size());

        long pointsFromAggregation = viewPort.getWidth() * 4;
        long pointsFromRaw = (to - from) / dataset.getSamplingInterval();   
        
        if(pointsFromAggregation > pointsFromRaw * dataReductionRatio) {
            DataPoints missingDataPoints = null;
            LOG.info("Fetching missing raw data from data source");
            missingDataPoints = dataSource.getDataPoints(from, to, new ArrayList<Integer>(missingIntervalsPerMeasure.keySet()));
            LOG.info("Fetched missing raw data from data source");
            timeSeriesSpans = TimeSeriesSpanFactory.createRaw(missingDataPoints, missingIntervalsPerMeasure);
        }
        else {
            for(int measure : aggFactors.keySet()) {
                int noOfGroups = aggFactors.get(measure) * viewPort.getWidth();
                numberOfGroups.put(measure, noOfGroups);
                aggregateIntervals.put(measure, (to - from) / numberOfGroups.get(measure));
            }
            AggregatedDataPoints missingDataPoints = null;
            LOG.info("Fetching missing data from data source");
            missingDataPoints = dataSource.getAggregatedDataPoints(from, to, missingIntervalsPerMeasure, numberOfGroups, queryMethod);
            LOG.info("Fetched missing data from data source");
            timeSeriesSpans = TimeSeriesSpanFactory.createAggregate(missingDataPoints, missingIntervalsPerMeasure, aggregateIntervals);
        }
        return timeSeriesSpans;
    }

    private int getPixelColumnForTimestamp(long timestamp, long from, long to, int width) {
        long aggregateInterval = (to - from) / width;
        return (int) ((timestamp - from) / aggregateInterval);
    }

    private void addAggregatedDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, AggregatedDataPoint aggregatedDataPoint) {
        int pixelColumnIndex = getPixelColumnForTimestamp(aggregatedDataPoint.getFrom(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addAggregatedDataPoint(aggregatedDataPoint);
        }
        // Since we only consider spans with intervals smaller than the pixel column interval, we know that the data point will not overlap more than two pixel columns.
        if (pixelColumnIndex <  viewPort.getWidth() - 1 && pixelColumns.get(pixelColumnIndex + 1).overlaps(aggregatedDataPoint)) {
            // If the next pixel column overlaps the data point, then we need to add the data point to the next pixel column as well.
            pixelColumns.get(pixelColumnIndex + 1).addAggregatedDataPoint(aggregatedDataPoint);
        }
    }

    private void addDataPointToPixelColumns(long from, long to, ViewPort viewPort, List<PixelColumn> pixelColumns, DataPoint dataPoint){
        int pixelColumnIndex = getPixelColumnForTimestamp(dataPoint.getTimestamp(), from, to, viewPort.getWidth());
        if (pixelColumnIndex < viewPort.getWidth()) {
            pixelColumns.get(pixelColumnIndex).addDataPoint(dataPoint);
        }
    }

    public QueryExecutor getQueryExecutor() {
        return queryExecutor;
    }
}
