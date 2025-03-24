package gr.imsi.athenarc.visual.middleware.domain;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An object for computing aggregate statistics for multi-variate time series data points.
 *
 * @implNote This implementation is not thread safe. However, it is safe to use
 * {@link Collectors#summarizingDouble(java.util.function.ToDoubleFunction)
 * Collectors.summarizingDouble()} on a parallel stream, because the parallel
 * implementation of {@link java.util.stream.Stream#collect Stream.collect()}
 * provides the necessary partitioning, isolation, and merging of results for
 * safe and efficient parallel execution.
 * @since 1.8
 */
public class StatsAggregator implements Consumer<DataPoint>, Stats, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StatsAggregator.class);
    private static final long serialVersionUID = 1L;

    protected int count;
    protected double sum;
    protected double minValue;
    protected long minTimestamp;
    protected double maxValue;
    protected long maxTimestamp;
    protected double firstValue;
    protected long firstTimestamp;
    protected double lastValue;
    protected long lastTimestamp;

    public StatsAggregator() {
        clear();
    }

    public void clear() {
        count = 0;
        sum = 0;
        minValue = Double.POSITIVE_INFINITY;
        minTimestamp = -1L;
        maxValue = Double.NEGATIVE_INFINITY;
        maxTimestamp = -1L;
        firstTimestamp =  Long.MAX_VALUE;
        lastTimestamp = -1L;
    }


    /**
     * Adds another datapoint into the summary information.
     *
     * @param dataPoint the dataPoint
     */
    @Override
    public void accept(DataPoint dataPoint) {
        if (dataPoint == null) {
            LOG.warn("Null data point encountered, skipping");
            return;
        }
        
        if (dataPoint instanceof AggregatedDataPoint) {
            accept((AggregatedDataPoint) dataPoint);
            return;
        }

        double value = dataPoint.getValue();
        sum += value;
        minValue = Math.min(minValue, value);
        if (minValue == value) {
            minTimestamp = dataPoint.getTimestamp();
        }
        maxValue = Math.max(maxValue, value);
        if (maxValue == value) {
            maxTimestamp = dataPoint.getTimestamp();
        }
        if (firstTimestamp > dataPoint.getTimestamp()) {
            firstValue = value;
            firstTimestamp = dataPoint.getTimestamp();
        }
        if (lastTimestamp < dataPoint.getTimestamp()) {
            lastValue = value;
            lastTimestamp = dataPoint.getTimestamp();
        }
        count++;
    }

    public void accept(AggregatedDataPoint dataPoint) {
        if (dataPoint == null || dataPoint.getStats() == null) {
            LOG.warn("Null aggregated data point or stats encountered, skipping");
            return;
        }
        
        Stats stats = dataPoint.getStats();
        if (dataPoint.getCount() > 0) {
            sum += stats.getSum();
            minValue = Math.min(minValue, stats.getMinValue());
            if (minValue == stats.getMinValue()) {
                minTimestamp = stats.getMinTimestamp();
            }
            maxValue = Math.max(maxValue, stats.getMaxValue());
            if (maxValue == stats.getMaxValue()) {
                maxTimestamp = stats.getMaxTimestamp();
            }
            if (firstTimestamp > stats.getFirstTimestamp()) {
                firstValue = stats.getFirstValue();
                firstTimestamp = stats.getFirstTimestamp();
            }
            if (lastTimestamp < stats.getLastTimestamp()) {
                lastValue = stats.getLastValue();
                lastTimestamp = stats.getLastTimestamp();
            }
            count += dataPoint.getCount();
        }
    }


    /**
     * Combines the state of a {@code Stats} instance into this
     * StatsAggregator.
     *
     * @param other another {@code Stats}
     * @throws IllegalArgumentException if the other Stats instance does not have the same measures as this StatsAggregator
     */
    public void combine(Stats other) {
        if (other == null) {
            LOG.warn("Null stats encountered in combine operation, skipping");
            return;
        }
        
        if(other.getCount() > 0) {
            sum += other.getSum();
            minValue = Math.min(minValue, other.getMinValue());
            if (minValue == other.getMinValue()) {
                minTimestamp = other.getMinTimestamp();
            }
            maxValue = Math.max(maxValue, other.getMaxValue());
            if (maxValue == other.getMaxValue()) {
                maxTimestamp = other.getMaxTimestamp();
            }
            if (firstTimestamp > other.getFirstTimestamp()) {
                firstValue = other.getFirstValue();
                firstTimestamp = other.getFirstTimestamp();
            }
            if (lastTimestamp < other.getLastTimestamp()) {
                lastValue = other.getLastValue();
                lastTimestamp = other.getLastTimestamp();
            }
            count += other.getCount();
        }
    }


    @Override
    public int getCount() {
        return count;
    }

    @Override
    public double getSum() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return sum;
    }

    @Override
    public double getMinValue() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return minValue;
    }

    @Override
    public double getMaxValue() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return maxValue;
    }

    @Override
    public double getAverageValue() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return sum / count;
    }

    @Override
    public long getMinTimestamp() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return minTimestamp;
    }

    @Override
    public long getMaxTimestamp() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return maxTimestamp;
    }

    @Override
    public double getFirstValue() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstValue;
    }

    @Override
    public long getFirstTimestamp() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return firstTimestamp;
    }

    @Override
    public double getLastValue() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastValue;
    }

    @Override
    public long getLastTimestamp() {
        if (count == 0) {
            throw new IllegalStateException("No data points added to this stats aggregator yet.");
        }
        return lastTimestamp;
    }

    public StatsAggregator clone() {
        StatsAggregator statsAggregator = new StatsAggregator();
        statsAggregator.combine(this);
        return statsAggregator;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        if (count == 0) {
            return "No data points";
        }
        return "Stats{count=" + count + 
               ", avg=" + getAverageValue() + 
               ", min=" + minValue + 
               ", max=" + maxValue + "}";
    }
}
