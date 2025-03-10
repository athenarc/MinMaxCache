package gr.imsi.athenarc.visual.middleware.datasource.query;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

/**
 * Represents a time series data source query
 */
public abstract class DataSourceQuery implements TimeInterval {

    final long from;
    final long to;

    final Map<String, List<TimeInterval>> missingIntervalsPerMeasure;

    Map<String, Integer> numberOfGroups;

    Map<String, Long> aggregateIntervals;

    /**
     * Creates a new instance of {@link DataSourceQuery}
     *
     * @param from           The start time of the time interval that was requested
     * @param to             The end time of the time interval that was requested
     * @param missingIntervalsPerMeasure         The actual sub-ranges that are missing from the cache and need to be fetched for each measure
     * @param numberOfGroups The number of groups to aggregate the data points into per measure
     */
    public DataSourceQuery(long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasure, Map<String, Integer> numberOfGroups) {
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
        this.numberOfGroups = numberOfGroups;
        this.aggregateIntervals = new HashMap<>(numberOfGroups.size());
        for(String measure : numberOfGroups.keySet()){
            this.aggregateIntervals.put(measure, (to - from) / numberOfGroups.get(measure));
        }
    }

    public DataSourceQuery(long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasure) {
        this.from = from;
        this.to = to;
        this.missingIntervalsPerMeasure = missingIntervalsPerMeasure;
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    @Override
    public String getFromDate() {
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    public abstract String m4QuerySkeleton();

    public abstract String minMaxQuerySkeleton();

    public abstract String rawQuerySkeleton();

    public Map<String, List<TimeInterval>> getMissingIntervalsPerMeasure() {
        return missingIntervalsPerMeasure;
    }

    public Map<String, Integer> getNumberOfGroups() {
        return numberOfGroups;
    }

    public Map<String, Long> getAggregateIntervals() {
        return aggregateIntervals;
    }

    /**
     *
     * @return number of measures times their time intervals. Equals to the number of distinct time series spans that will be fetched
     */
    public int getNoOfQueries() {
        return this.getMissingIntervalsPerMeasure().size() * this.getMissingIntervalsPerMeasure().values().stream().mapToInt(List::size).sum();

    }
}


