package gr.imsi.athenarc.visual.middleware.domain;


import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;

/**
 * Represents an immutable single univariate data point with a single value and a timestamp.
 */
public class ImmutableDataPoint implements DataPoint {

    private final long timestamp;

    private final double value;

    // The measure of this data point
    private final int measure;

    public ImmutableDataPoint(final long timestamp, final double value, int measure) {
        this.timestamp = timestamp;
        this.value = value;
        this.measure = measure;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public int getMeasure() {
        return measure;
    }


    @Override
    public String toString() {
        return "{" + timestamp + ", " + DateTimeUtil.format(timestamp) +
                ", " + value +
                '}';
    }
}
    
