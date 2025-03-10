package gr.imsi.athenarc.visual.middleware.datasource.iterator.m4;

import gr.imsi.athenarc.visual.middleware.datasource.iterator.PostgreSQLIterator;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.DateTimeUtil;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableAggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.StatsAggregator;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgreSQLM4DataPointsIterator extends PostgreSQLIterator<AggregatedDataPoint> {

    private final List<TimeInterval> unionTimeIntervals;
    private final Map<String, Long> aggregateIntervals;
    private final Map<String, Integer> measuresMap;

    public PostgreSQLM4DataPointsIterator(ResultSet resultSet,
                                        Map<String, List<TimeInterval>> missingIntervalsPerMeasure,
                                        Map<String, Long> aggregateIntervals,
                                        Map<String, Integer> measuresMap) {
        super(resultSet);
        this.unionTimeIntervals = missingIntervalsPerMeasure.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        this.aggregateIntervals = aggregateIntervals;
        this.measuresMap = measuresMap;
    }

    @Override
    protected AggregatedDataPoint getNext() {
        try {
            StatsAggregator statsAggregator = new StatsAggregator();
            String measure = resultSet.getString(1);
            long t_min = resultSet.getLong(2);
            long t_max = resultSet.getLong(3);
            double value = resultSet.getDouble(4);
            int k = resultSet.getInt(5);
            int unionGroup = resultSet.getInt(6); // signifies the union id
            Long aggregateInterval = aggregateIntervals.get(measure);

            TimeInterval correspondingInterval = unionTimeIntervals.get(unionGroup);

            long firstTimestamp = correspondingInterval.getFrom() + k * aggregateInterval;
            long lastTimestamp = correspondingInterval.getFrom() + ((k + 1) * aggregateInterval);
            if(firstTimestamp + aggregateInterval > correspondingInterval.getTo()) {
                lastTimestamp = correspondingInterval.getTo();
            }
            DataPoint dataPoint1 = new ImmutableDataPoint(t_min, value, measuresMap.get(measure));
            DataPoint dataPoint2 = new ImmutableDataPoint(t_max, value, measuresMap.get(measure));
            statsAggregator.accept(dataPoint1);
            statsAggregator.accept(dataPoint2);
            LOG.debug("Created aggregate Datapoint {} - {} with firsts: {}, last: {}, min: {} and max: {} ",
                    DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp),
                    statsAggregator.getFirstValue(),
                    statsAggregator.getLastValue(),
                    statsAggregator.getMinValue(),
                    statsAggregator.getMaxValue());
            return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, measuresMap.get(measure), statsAggregator);
        } catch (SQLException e) {
            LOG.error("Error retrieving next M4 data point", e);
            return null;
        }
    }
}

