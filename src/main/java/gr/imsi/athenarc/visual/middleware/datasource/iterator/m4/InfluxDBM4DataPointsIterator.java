package gr.imsi.athenarc.visual.middleware.datasource.iterator.m4;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.InfluxDBIterator;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;

import java.util.List;
import java.util.Map;

public class InfluxDBM4DataPointsIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private static final int POINTS_PER_AGGREGATE = 4;

    public InfluxDBM4DataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap) {
        super(tables, measuresMap);
    }

    @Override
    protected AggregatedDataPoint getNext() {
        StatsAggregator statsAggregator = new StatsAggregator();
        String measure = "";
        
        for (int i = 0; i < POINTS_PER_AGGREGATE && current < currentSize; i++) {
            FluxRecord record = currentRecords.get(current);
            measure = record.getField();
            
            Object value = record.getValue();
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                long timestamp = getTimestampFromRecord(record, "_time");
                
                statsAggregator.accept(new ImmutableDataPoint(
                    timestamp, 
                    doubleValue, 
                    measuresMap.get(measure)
                ));
            }
            
            currentGroupTimestamp = getTimestampFromRecord(record, "_stop");
            current++;
        }

        AggregatedDataPoint point = new ImmutableAggregatedDataPoint(
            groupTimestamp, 
            currentGroupTimestamp, 
            measuresMap.get(measure), 
            statsAggregator
        );

        logAggregatedPoint(point);
        groupTimestamp = currentGroupTimestamp;
        
        return point;
    }

    private void logAggregatedPoint(AggregatedDataPoint point) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created aggregate Datapoint {} - {} with first: {}, last: {}, min: {} and max: {}",
                point.getFrom(), point.getTo(),
                point.getStats().getFirstValue(),
                point.getStats().getLastValue(),
                point.getStats().getMinValue(),
                point.getStats().getMaxValue());
        }
    }
}
