package gr.imsi.athenarc.visual.middleware.domain;

/**
 * A representation of aggregate statistics for multi-variate time series data points.
 */
public interface Stats {
    public int getCount();

    public double getSum();

    public double getMinValue();
    public long getMinTimestamp();

    public double getMaxValue();

    public long getMaxTimestamp();

    public double getFirstValue();

    public long getFirstTimestamp();

    public double getLastValue();

    public long getLastTimestamp();


    public double getAverageValue();

    default DataPoint getMinDataPoint() {
        return new ImmutableDataPoint(getMinTimestamp(), getMinValue());
    }

    default DataPoint getMaxDataPoint() {
        return new ImmutableDataPoint(getMaxTimestamp(), getMaxValue());
    }

    default DataPoint getFirstDataPoint() {
        return new ImmutableDataPoint(getFirstTimestamp(), getFirstValue());
    }

    default DataPoint getLastDataPoint() {
        return new ImmutableDataPoint(getLastTimestamp(), getLastValue());
    }



    default String getString(int measure) {
        return "{" +
                "measure=" + measure +
                ", count=" + getCount() +
                ", sum=" + getSum() +
                ", min=" + getMinValue() +
                ", minTimestamp=" + getMinTimestamp() +
                ", max=" + getMaxValue() +
                ", maxTimestamp=" + getMaxTimestamp() +
                ", first=" + getFirstValue() +
                ", firstTimestamp=" + getFirstTimestamp() +
                ", last=" + getLastValue() +
                ", lastTimestamp=" + getLastTimestamp() +
                ", average=" + getAverageValue() +
                '}';
    }

    default String toString(int measure) {
        return "{" +
                "measure=" + measure +
                ", count=" + getCount() +
                ", sum=" + getSum() +
                ", min=" + getMinValue() +
                ", minTimestamp=" + getMinTimestamp() +
                ", max=" + getMaxValue() +
                ", maxTimestamp=" + getMaxTimestamp() +
                ", first=" + getFirstValue() +
                ", firstTimestamp=" + getFirstTimestamp() +
                ", last=" + getLastValue() +
                ", lastTimestamp=" + getLastTimestamp() +
                ", average=" + getAverageValue() +
                '}';
    }

}
