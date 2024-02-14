package gr.imsi.athenarc.visual.middleware.domain;


/**
 * Represents a single, immutable multi-measure data point with a number of values and a timestamp.
 */
public class ImmutableAggregatedDataPoint implements AggregatedDataPoint {

    /**
     * Creates a new ImmutableAggregatedDataPoint from the given AggregatedDataPoint.
     */
    public static ImmutableAggregatedDataPoint fromAggregatedDataPoint(AggregatedDataPoint aggregatedDataPoint) {
        return new ImmutableAggregatedDataPoint(aggregatedDataPoint.getFrom(), aggregatedDataPoint.getTo(),
                aggregatedDataPoint.getMeasure(), aggregatedDataPoint.getStats());
    }

    // The start timestamp of this data point (inclusive)
    private final long from;

    // The end timestamp of this data point (exclusive)
    private final long to;

    // The measure of this data point
    private final int measure;

    /**
     * An array of double values, corresponding to a set of measures.
     * The mapping of each value to a measure is handled elsewhere,
     * depending on the specific case.
     */
    private final Stats stats;


    /**
     * Creates a new ImmutableAggregatedDataPoint with the given from, to, and stats.
     * @param from The start timestamp (inclusive)
     * @param to The end timestamp (exclusive)
     * @param stats The stats of this aggregated data point
     */
    public ImmutableAggregatedDataPoint(final long from, final long to, final int measure, final Stats stats) {
        this.from = from;
        this.to = to;
        this.measure = measure;
        this.stats = stats;

    }

    @Override
    public long getTimestamp() {
        return from;
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
    public double getValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getCount() {
        return stats.getCount();
    }

    @Override
    public Stats getStats() {
        return stats;
    }

    @Override
    public int getMeasure() {
        return measure;
    }

    @Override
    public String toString() {
        return getString();
    }

}
