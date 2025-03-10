package gr.imsi.athenarc.visual.middleware.datasource.iterator.m4;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

public class CsvM4DataPointsIterator implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvM4DataPointsIterator.class);

    private final Iterator<String[]> csvDataPointsIterator;
    private final Map<String, List<TimeInterval>> intervalsPerMeasure;
    private final Map<String, Long> aggregateIntervals;
    private final int timeColumnIndex;
    private final DateTimeFormatter formatter;
    private final Map<String, Integer> measureColumnIndices;

    // State variables per measure
    private final Map<String, MeasureAggregationState> measureStates;
    private final PriorityQueue<AggregatedDataPoint> nextDataPointsQueue;

    public CsvM4DataPointsIterator(
            Iterable<String[]> csvDataPoints,
            Map<String, List<TimeInterval>> intervalsPerMeasure,
            Map<String, Integer> measureColumnIndices,
            Map<String, Long> aggregateIntervals,
            int timeColumnIndex,
            DateTimeFormatter formatter) {
        this.csvDataPointsIterator = csvDataPoints.iterator();
        this.intervalsPerMeasure = intervalsPerMeasure;
        this.measureColumnIndices = measureColumnIndices;
        this.aggregateIntervals = aggregateIntervals;
        this.timeColumnIndex = timeColumnIndex;
        this.formatter = formatter;

        // Initialize state per measure
        this.measureStates = new HashMap<>();
        for (String measure : intervalsPerMeasure.keySet()) {
            measureStates.put(measure, new MeasureAggregationState(measure));
        }

        // Initialize priority queue to sort data points by measure and timestamp
        this.nextDataPointsQueue = new PriorityQueue<>(
            Comparator.comparing((AggregatedDataPoint dp) -> dp.getMeasure())
                      .thenComparingLong(AggregatedDataPoint::getTimestamp)
        );

        // Start processing
        advance();
    }

    @Override
    public boolean hasNext() {
        return !nextDataPointsQueue.isEmpty();
    }

    @Override
    public AggregatedDataPoint next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        AggregatedDataPoint aggregatedDataPoint = nextDataPointsQueue.poll();
        LOG.debug("Agg point: {}", aggregatedDataPoint);
        return aggregatedDataPoint;
        // return nextDataPointsQueue.poll();
    }

    private void advance() {
        try {
            // Iterate over CSV data once
            while (csvDataPointsIterator.hasNext()) {
                String[] row = csvDataPointsIterator.next();
                // Get timestamp
                String timestampStr = row[timeColumnIndex];
                long timestamp = DateTimeUtil.parseDateTimeString(timestampStr, formatter);

                // Process each measure
                for (Map.Entry<String, MeasureAggregationState> entry : measureStates.entrySet()) {
                    String measure = entry.getKey();
                    MeasureAggregationState state = entry.getValue();
                    int measureIndex = measureColumnIndices.get(measure);

                    // Check if timestamp is within any of the measure's intervals
                    if (!state.isTimestampWithinCurrentInterval(timestamp)) {
                        if (!state.moveToNextInterval(timestamp)) {
                            continue; // No more intervals for this measure
                        }
                    }

                    // Process sub-intervals
                    while (timestamp >= state.currentSubIntervalEnd) {
                        // Create aggregated data point for the completed sub-interval
                        AggregatedDataPoint dataPoint = state.createAggregatedDataPoint();
                        if (dataPoint != null) {
                            nextDataPointsQueue.offer(dataPoint);
                        }
                        if (!state.moveToNextSubInterval()) {
                            break; // No more sub-intervals for this interval
                        }
                    }

                    // Check if timestamp is within current sub-interval
                    if (timestamp >= state.currentSubIntervalStart && timestamp < state.currentSubIntervalEnd) {
                        // Process measure value
                        if (measureIndex < row.length) {
                            String valueStr = row[measureIndex];
                            try {
                                double value = Double.parseDouble(valueStr);
                                state.aggregator.accept(new ImmutableDataPoint(timestamp, value, measureIndex));
                            } catch (NumberFormatException e) {
                                // Handle invalid number formats
                            }
                        }
                    }
                }
            }

            // After processing all CSV data, collect any remaining data points
            for (MeasureAggregationState state : measureStates.values()) {
                // Complete any pending sub-intervals
                AggregatedDataPoint dataPoint = state.createAggregatedDataPoint();
                if (dataPoint != null) {
                    nextDataPointsQueue.offer(dataPoint);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error during aggregation", e);
        }
    }

    private class MeasureAggregationState {
        final String measure;
        final List<TimeInterval> intervals;
        final long aggregateInterval;
        final int measureIndex;
        private StatsAggregator aggregator;
        int currentIntervalIndex;
        long currentSubIntervalStart;
        long currentSubIntervalEnd;
    
        public MeasureAggregationState(String measure) {
            this.measure = measure;
            this.intervals = intervalsPerMeasure.get(measure);
            this.aggregateInterval = aggregateIntervals.get(measure);
            this.measureIndex = measureColumnIndices.get(measure);
            this.currentIntervalIndex = -1;
            this.aggregator = null; // Initialize as null
            moveToNextInterval(Long.MIN_VALUE);
        }
    
        boolean isTimestampWithinCurrentInterval(long timestamp) {
            if (currentIntervalIndex >= intervals.size()) {
                return false;
            }
            TimeInterval currentInterval = intervals.get(currentIntervalIndex);
            return timestamp >= currentInterval.getFrom() && timestamp < currentInterval.getTo();
        }
    
        boolean moveToNextInterval(long timestamp) {
            currentIntervalIndex++;
            while (currentIntervalIndex < intervals.size()) {
                TimeInterval currentInterval = intervals.get(currentIntervalIndex);
                if (currentInterval.getTo() > timestamp) {
                    currentSubIntervalStart = currentInterval.getFrom();
                    currentSubIntervalEnd = Math.min(currentSubIntervalStart + aggregateInterval, currentInterval.getTo());
                    // Create a new aggregator instance
                    aggregator = new StatsAggregator();
                    
                    return true;
                }
                currentIntervalIndex++;
            }
            return false;
        }
    
        boolean moveToNextSubInterval() {
            TimeInterval currentInterval = intervals.get(currentIntervalIndex);
            if (currentSubIntervalEnd >= currentInterval.getTo()) {
                return false; // No more sub-intervals in current interval
            } else {
                // Move to next sub-interval within the current interval
                currentSubIntervalStart = currentSubIntervalEnd;
                currentSubIntervalEnd = Math.min(currentSubIntervalStart + aggregateInterval, currentInterval.getTo());
                // Create a new aggregator instance
                aggregator = new StatsAggregator();
                return true;
            }
        }
    
        AggregatedDataPoint createAggregatedDataPoint() {
            if (aggregator.getCount() > 0) {
                return new ImmutableAggregatedDataPoint(
                        aggregator.getFirstTimestamp(),
                        aggregator.getLastTimestamp(),
                        measureIndex,
                        aggregator
                );
            }
            return null;
        }
    }
}
