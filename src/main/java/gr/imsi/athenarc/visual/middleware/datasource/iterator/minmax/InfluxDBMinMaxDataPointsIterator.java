package gr.imsi.athenarc.visual.middleware.datasource.iterator.minmax;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.InfluxDBIterator;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.NonTimestampedStatsAggregator;

import java.util.List;
import java.util.Map;

public class InfluxDBMinMaxDataPointsIterator extends InfluxDBIterator<AggregatedDataPoint> {

    private static final int POINTS_PER_AGGREGATE = 2;

    public InfluxDBMinMaxDataPointsIterator(List<FluxTable> tables, Map<String, Integer> measuresMap) {
        super(tables, measuresMap);
    }

    @Override
    protected AggregatedDataPoint getNext() {
        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator();
        String measure = "";
        
        for (int i = 0; i < POINTS_PER_AGGREGATE && current < currentSize; i++) {
            FluxRecord record = currentRecords.get(current);
            measure = record.getField();
            
            Object value = record.getValue();
            if (value instanceof Number) {
                statsAggregator.accept(((Number) value).doubleValue());
            }
            current++;
        }

        updateGroupTimestamps();
        
        if (statsAggregator.getCount() == 0 && hasNext()) {
            return next();
        }

        statsAggregator.setFrom(groupTimestamp);
        statsAggregator.setTo(currentGroupTimestamp);

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

    private void logAggregatedPoint(AggregatedDataPoint point, NonTimestampedStatsAggregator stats) {
        LOG.debug("Created aggregate Datapoint {} - {} with min: {} and max: {}",
            DateTimeUtil.format(point.getFrom()),
            DateTimeUtil.format(point.getTo()),
            stats.getMinValue(),
            stats.getMaxValue());
    }
}