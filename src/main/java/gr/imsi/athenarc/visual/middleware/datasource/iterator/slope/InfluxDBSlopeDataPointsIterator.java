package gr.imsi.athenarc.visual.middleware.datasource.iterator.slope;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.InfluxDBIterator;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;

import java.util.List;
import java.util.Map;

public class InfluxDBSlopeDataPointsIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private static final int POINTS_PER_AGGREGATE = 2;

    public InfluxDBSlopeDataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap) {
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
            current++;
        }

        updateGroupTimestamps();
        
        if (statsAggregator.getCount() == 0 && hasNext()) {
            return next();
        }

        AggregatedDataPoint point = new ImmutableAggregatedDataPoint(
            groupTimestamp, 
            currentGroupTimestamp, 
            measuresMap.get(measure), 
            statsAggregator
        );

        logAggregatedPoint(point, statsAggregator);
        groupTimestamp = currentGroupTimestamp;
        
        return point;
    }

    private void updateGroupTimestamps() {
        if (current == currentSize) {
            currentGroupTimestamp = getTimestampFromRecord(currentRecords.get(currentSize - 1), "_stop");
        } else {
            currentGroupTimestamp = getTimestampFromRecord(currentRecords.get(current), "_time");
        }
    }

    private void logAggregatedPoint(AggregatedDataPoint point, StatsAggregator stats) {
        LOG.debug("Created aggregate Datapoint {} - {} first: {} and last {}",
            DateTimeUtil.format(point.getFrom()),
            DateTimeUtil.format(point.getTo()),
            stats.getFirstValue(),
            stats.getLastValue());
    }
}