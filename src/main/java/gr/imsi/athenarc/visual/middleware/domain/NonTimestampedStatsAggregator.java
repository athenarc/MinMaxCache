package gr.imsi.athenarc.visual.middleware.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * An implementation of the Stats interface that calculates stats within a fixed interval [from, to).
 * This class only takes into account values and not timestamps of data points.
 * As timestamps the middle of the interval is used for the data points with the min and max values,
 * and the from and to timestamps for the first and last data points.
 */
public class NonTimestampedStatsAggregator implements Stats, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StatsAggregator.class);

    private long from;
    private long to;

    private int count = 0;
    private double sum;
    private double minValue;
    private double maxValue;

    public NonTimestampedStatsAggregator() {
        clear();
    }

    public void clear() {
        count = 0;
        sum = 0d;
        minValue = Double.POSITIVE_INFINITY;
        maxValue = Double.NEGATIVE_INFINITY;
    }

    public void accept(double value) {
        ++count;
        sum += value;
        minValue = Math.min(minValue, value);
        maxValue = Math.max(maxValue, value);
    }

    /**
     * Combines the state of a {@code Stats} instance into this
     * StatsAggregator.
     *
     * @param other another {@code Stats}
     * Handles the case of different measures
     */
    public void combine(Stats other) {
        if(other.getCount() != 0) {
            sum += other.getSum();
            minValue = Math.min(minValue, other.getMinValue());
            maxValue = Math.max(maxValue, other.getMaxValue());
            count += other.getCount();
        }
    }


    public int getCount() {
        return count;
    }

    public double getSum() {
        return sum;
    }

    public double getMinValue() {
        return minValue;
    }

    @Override
    public long getMinTimestamp() {
        return (from + to) / 2;
    }

    public double getMaxValue() {
        return maxValue;
    }

    @Override
    public long getMaxTimestamp() {
        return (from + to) / 2;
    }

    @Override
    public double getFirstValue() {
        return (getMinValue() + getMaxValue()) / 2;
    }

    @Override
    public long getFirstTimestamp() {
        return from + 1;
    }

    @Override
    public double getLastValue() {
        return (getMinValue() + getMaxValue()) / 2;
    }

    @Override
    public long getLastTimestamp() {
        return to - 1;
    }

    @Override
    public double getAverageValue() {
        return getSum() / getCount();
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "NonTimestampedStatsAggregator [from=" + from + ", to=" + to + ", count=" + count + ", sum=" + sum
                + ", minValue=" + minValue + ", maxValue=" + maxValue + "]";
    }

    
}
