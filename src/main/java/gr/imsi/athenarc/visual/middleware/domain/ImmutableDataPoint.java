package gr.imsi.athenarc.visual.middleware.domain;


import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;

/**
 * Represents an immutable single univariate data point with a single value and a timestamp.
 */
public class ImmutableDataPoint implements DataPoint {

    private final long timestamp;

    private final double value;

    public ImmutableDataPoint(final long timestamp, final double value) {
        this.timestamp = timestamp;
        this.value = value;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "{" + timestamp + ", " + DateTimeUtil.format(timestamp) +
                ", " + value +
                '}';
    }
}
