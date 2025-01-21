package gr.imsi.athenarc.visual.middleware.cache;
import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TimeSeriesSpanFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesSpanFactory.class);

    /**
     * Read from iterators and create raw time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * The raw time series span needs to first collect the raw datapoints and then be built. This is because the sampling interval may vary.
     * @param dataPoints fetched raw datapoints
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @return A list of RawTimeSeriesSpan for each measure
     */
    protected static Map<Integer, List<TimeSeriesSpan>> createRaw(DataPoints dataPoints, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure){
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<DataPoint> it = dataPoints.iterator();
        DataPoint dataPoint = null;
        LOG.info("{}", missingIntervalsPerMeasure);
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            boolean changed = false;
            for(TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                RawTimeSeriesSpan timeSeriesSpan = new RawTimeSeriesSpan(range.getFrom(), range.getTo(), measure);
                List<DataPoint> dataPointsList = new ArrayList<>();
                while (it.hasNext()) {
                    if (!changed){ // if there is a change keep the previous datapoint to process
                        if(it.hasNext()) dataPoint = it.next();
                        else break;
                    }
                    if (dataPoint.getTimestamp() < range.getFrom() || dataPoint.getTimestamp() >= range.getTo()
                        || dataPoint.getMeasure() != measure) {
                        changed = true;
                        break;
                    }
                    else{
                        changed = false;
                        // LOG.info("Adding {} between {}-{}", dataPoint.getTimestamp(), range.getFrom(), range.getTo());
                        dataPointsList.add(dataPoint);
                    }   
                }
                timeSeriesSpan.build(dataPointsList);
                timeSeriesSpansForMeasure.add(timeSeriesSpan);
                LOG.info("Created raw time series span: {}", timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        return spans;
    }

    /**
     * Read from iterators and create time series spans.
     * All spans take account of the residual interval left from a not exact division with the aggregate interval.
     * @param aggregatedDataPoints fetched aggregated data points
     * @param missingIntervalsPerMeasure  list of ranges for each measure that this points belong to
     * @param aggregateIntervalsPerMeasure aggregate intervals with which to fetch data for each measure
     * @return A list of AggregateTimeSeriesSpan for each measure
     */
    protected static Map<Integer, List<TimeSeriesSpan>> createAggregate(AggregatedDataPoints aggregatedDataPoints,
                                                                     Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                                     Map<Integer, Long> aggregateIntervalsPerMeasure) {
        Map<Integer, List<TimeSeriesSpan>> spans = new HashMap<>();
        Iterator<AggregatedDataPoint> it = aggregatedDataPoints.iterator();
        AggregatedDataPoint aggregatedDataPoint = null;
        boolean changed = false;
        for (Integer measure : missingIntervalsPerMeasure.keySet()) {
            long aggregateInterval = aggregateIntervalsPerMeasure.get(measure);
            List<TimeSeriesSpan> timeSeriesSpansForMeasure = new ArrayList<>();
            for (TimeInterval range : missingIntervalsPerMeasure.get(measure)) {
                int j = 0;
                AggregateTimeSeriesSpan timeSeriesSpan = new AggregateTimeSeriesSpan(range.getFrom(), range.getTo(), measure, aggregateInterval);
                // This is to handle missing fetched data.
                // There is not a 1-1 mapping between the fetched aggregate data and the time series span we are creating.
                // Postgres omits results if there is no data in the group (InfluxDB has a fill empty clause and handles this, so we create the empty data points in the iterator).
                // We use this solution to see when the time series span changes.
                while (j < (timeSeriesSpan.getSize() - 1)) {
                    if (!changed){ // if there is a change keep the previous datapoint to process
                        if(it.hasNext()) aggregatedDataPoint = it.next();
                        else break;
                    }
                    if (aggregatedDataPoint.getTimestamp() < range.getFrom()
                            || aggregatedDataPoint.getTimestamp() >= range.getTo() || aggregatedDataPoint.getMeasure() != measure) {
                        changed = true;
                        break;
                    }
                    else {
                        changed = false;
                        j = DateTimeUtil.indexInInterval(range.getFrom(), range.getTo(), aggregateInterval, aggregatedDataPoint.getTimestamp());
                        LOG.debug("Adding {} between {}-{} with aggregate interval {} for measure {} at position {}",
                                aggregatedDataPoint.getTimestamp(), range.getFrom(), range.getTo(), aggregateInterval, measure, j);
                        timeSeriesSpan.addAggregatedDataPoint(j, aggregatedDataPoint);
                    }
                }
                timeSeriesSpansForMeasure.add(timeSeriesSpan);
            }
            spans.put(measure, timeSeriesSpansForMeasure);
        }
        return spans;
    }
}


