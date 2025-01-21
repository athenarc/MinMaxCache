package gr.imsi.athenarc.visual.middleware.datasource.postgresql;

import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgreSQLAggregateDataPointsIteratorM4 implements Iterator<AggregatedDataPoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DataSource.class);

    private final ResultSet resultSet;
    private final List<TimeInterval> unionTimeIntervals;
    private final Map<String, Long> aggregateIntervals;

    private final Map<String, Integer> measuresMap;


    public PostgreSQLAggregateDataPointsIteratorM4(ResultSet resultSet,
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


    /*
       For each grouping k that comes from the query.
       Get the values, put them in a list.
       Then pass through the list to initialize the corresponding aggregator.
       A unionGroup is a subQuery based on the UNION of the query.
     */
    @Override
    public AggregatedDataPoint next() {
        try {
            String measure = resultSet.getString(1);
            long t_min = resultSet.getLong(2);
            long t_max = resultSet.getLong(3);
            double value = resultSet.getDouble(4);
            int k = resultSet.getInt(5);
            int unionGroup = resultSet.getInt(6); // signifies the union id
            Long aggregateInterval = aggregateIntervals.get(measure);

            StatsAggregator statsAggregator = new StatsAggregator();

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
            e.printStackTrace();
        }
        return null;
    }
}

