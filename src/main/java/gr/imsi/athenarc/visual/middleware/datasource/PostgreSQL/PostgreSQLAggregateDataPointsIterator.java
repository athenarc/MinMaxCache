package gr.imsi.athenarc.visual.middleware.datasource.PostgreSQL;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class PostgreSQLAggregateDataPointsIterator implements Iterator<AggregatedDataPoint> {


    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final ResultSet resultSet;
    private final List<TimeInterval> unionTimeIntervals;
    private final Map<String, Long> aggregateIntervals;
    private final Map<String, Integer> measuresMap;

    public PostgreSQLAggregateDataPointsIterator(ResultSet resultSet,
                                                 Map<String, List<TimeInterval>> missingIntervalsPerMeasure,
                                                 Map<String, Long> aggregateIntervals, Map<String, Integer> measuresMap) throws SQLException {
        this.resultSet = resultSet;
        this.unionTimeIntervals = missingIntervalsPerMeasure.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        this.aggregateIntervals = aggregateIntervals;
        this.measuresMap = measuresMap;
    }

    @Override
    public boolean hasNext() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public AggregatedDataPoint next() {
        NonTimestampedStatsAggregator statsAggregator = new NonTimestampedStatsAggregator();
        // get row data
        try {
            String measure = resultSet.getString(1);
            int k = resultSet.getInt(2);
            double v_min = resultSet.getDouble(3);
            double v_max = resultSet.getDouble(4);
            int unionGroup = resultSet.getInt(5); // signifies the union id

            Long aggregateInterval = aggregateIntervals.get(measure);

            TimeInterval correspondingInterval = unionTimeIntervals.get(unionGroup);

            long firstTimestamp = correspondingInterval.getFrom() + k * aggregateInterval;
            long lastTimestamp = correspondingInterval.getFrom() + ((k + 1) * aggregateInterval);
            if(firstTimestamp + aggregateInterval > correspondingInterval.getTo()) {
                lastTimestamp = correspondingInterval.getTo();
            }
            statsAggregator.setFrom(firstTimestamp);
            statsAggregator.setTo(lastTimestamp);
            statsAggregator.accept(v_min);
            statsAggregator.accept(v_max);

            LOG.debug("Created aggregate Datapoint {} - {} with min: {} and max: {} ",
                    DateTimeUtil.format(firstTimestamp), DateTimeUtil.format(lastTimestamp), statsAggregator.getMinValue(), statsAggregator.getMaxValue());
            return new ImmutableAggregatedDataPoint(firstTimestamp, lastTimestamp, measuresMap.get(measure), statsAggregator);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
