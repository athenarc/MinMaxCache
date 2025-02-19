package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * A {@link DataPoints} implementation that aggregates a series of consecutive
 * raw data points based on the specified aggregation interval.
 * For each aggregation interval included we store 5 doubles,
 * i.e. the sum, min and max aggregate values, 2 longs corresponding to the timestamp of the min and max value, as well as the corresponding
 * non-missing value counts.
 */
public class AggregateTimeSeriesSpan implements TimeSeriesSpan {

    private static final Logger LOG = LoggerFactory.getLogger(DataProcessor.class);

    private int measure;

    private int count;

    /**
     * The aggregate values for every window interval and for every measure.
     */
    private long[] aggregates;

    // The start time value of the span
    private long from;

    // The end time value of the span
    // Keep in mind that the end time is not included in the span,
    // and also that the end time is not necessarily aligned with the aggregation interval
    private long to;

    // The size of this span, corresponding to the number of aggregated window intervals represented by it
    private int size;

    /**
     * The fixed window that raw data points are grouped by in this span.
     * Note that due to rounding, the last group of the span may cover a larger time interval
     */
    private long aggregateInterval;


    private void initialize(long from, long to, long aggregateInterval, int measure) {
        this.size = DateTimeUtil.numberOfIntervals(from, to, aggregateInterval);
        this.from = from;
        this.to = to;
        this.aggregateInterval = aggregateInterval;
        LOG.debug("Initializing time series span ({},{}) measure = {} with size {}, aggregate interval {}", getFromDate(), getToDate(), measure, size, aggregateInterval);
        this.measure = measure;
        this.aggregates = new long[size * 5];
    }


    protected AggregateTimeSeriesSpan(long from, long to, int measure, long aggregateInterval) {
        initialize(from, to, aggregateInterval, measure);
    }

    protected void addAggregatedDataPoint(int i, AggregatedDataPoint aggregatedDataPoint) {
        Stats stats = aggregatedDataPoint.getStats();
        count += stats.getCount();
        if (stats.getCount() == 0) {
            return;
        }

        long minTimestamp = stats.getMinTimestamp();
        long maxTimestamp = stats.getMaxTimestamp();
        double minValue = stats.getMinValue();
        double maxValue = stats.getMaxValue();

        aggregates[5 * i] = Double.doubleToRawLongBits(stats.getSum());
        aggregates[5 * i + 1] = Double.doubleToRawLongBits(minValue);
        aggregates[5 * i + 2] = minTimestamp;
        aggregates[5 * i + 3] = Double.doubleToRawLongBits(maxValue);
        // not sure if this helps. we do it to keep the last timestamp in case of same values in the interval
        if (maxValue == stats.getLastValue()){
            aggregates[5 * i + 4] = stats.getLastTimestamp();
        } else {
            aggregates[5 * i + 4] = maxTimestamp;
        }
    }

    /**
     * Finds the index in the span in which the given timestamp should be.
     * If the timestamp is before the start of the span, the first index is returned.
     * If the timestamp is after the end of the span, the last index is returned.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = (int) ((timestamp - from) / aggregateInterval);
        if (index >= size) {
            return size - 1;
        } else if (index < 0) {
            return 0;
        }
        return index;
    }

    public int getSize() {
        return size;
    }

    public long getAggregateInterval() {
        return aggregateInterval;
    }

    public TimeInterval getResidual(){
        return new TimeRange(from + (aggregateInterval) * (size - 1), to);
    }
    /**
     * Returns an iterator over the aggregated data points in this span that fall within the given time range.
     *
     * @param queryStartTimestamp The start timestamp of the query.
     * @param queryEndTimestamp   The end timestamp of the query (not included).
     * @return The iterator.
     */
    public Iterator<AggregatedDataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) {
        return new TimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp);
    }


    public TimeRange getTimeRange() {
        return new TimeRange(getFrom(), getTo());
    }


    @Override
    public Iterator iterator() {
        return iterator(from, -1);
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
    public String toString() {
        return "{[" + getFromDate() + "(" + getFrom() + ")" +
                ", " + getToDate() + "(" + getTo() + ")" +
                "), size=" + size + ", measures =" + measure + "aggregateInterval=" + aggregateInterval + "}";
    }

    /**
     * Calculates the deep memory size of this instance.
     *
     * @return The deep memory size in bytes.
     */
    public long calculateDeepMemorySize() {
        // Memory overhead for an object in a 64-bit JVM
        final int OBJECT_OVERHEAD = 16;
        // Memory overhead for an array in a 64-bit JVM
        final int ARRAY_OVERHEAD = 20;
        // Memory usage of int in a 64-bit JVM
        final int INT_SIZE = 4;
        // Memory usage of long in a 64-bit JVM
        final int LONG_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 4;


        long aggregatesMemory = REF_SIZE + ARRAY_OVERHEAD + aggregates.length * (REF_SIZE + ARRAY_OVERHEAD + ((long) size * 5 * LONG_SIZE));

        long countsMemory = REF_SIZE + ARRAY_OVERHEAD + ((long) count * INT_SIZE);

        long aggregateIntervalMemory = 2 * REF_SIZE + OBJECT_OVERHEAD + LONG_SIZE;

        long deepMemorySize = REF_SIZE + OBJECT_OVERHEAD +
                aggregatesMemory + countsMemory + LONG_SIZE + INT_SIZE + aggregateIntervalMemory;


        return deepMemorySize;
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public int getMeasure() { return measure; }


/*    public TimeSeriesSpan rollup(AggregateInterval newAggregateInterval) {
        // Validate that the new aggregate interval is larger than the current one
        if (newAggregateInterval.toDuration().compareTo(this.aggregateInterval.toDuration()) <= 0) {
            throw new IllegalArgumentException("The new aggregate interval must be larger than the current one.");
        }
        return TimeSeriesSpanFactory.createFromRaw(this, newAggregateInterval);
    }*/

    private class TimeSeriesSpanIterator implements Iterator<AggregatedDataPoint>, AggregatedDataPoint {

        private Iterator<Integer> internalIt;

        private long timestamp;

        private int currentIndex = -1;

        public TimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            internalIt = IntStream.range(getIndex(queryStartTimestamp), queryEndTimestamp >= 0 ? getIndex(queryEndTimestamp - 1) + 1 : size)
                    .iterator();
        }

        @Override
        public boolean hasNext() {
            return internalIt.hasNext();
        }

        @Override
        public AggregatedDataPoint next() {
            currentIndex = internalIt.next();
            timestamp = from + currentIndex * aggregateInterval;
            return this;
        }

        @Override
        public int getCount() {
            return count;
        }

        @Override
        public Stats getStats() {
            return new Stats() {

                private int index = currentIndex;

                @Override
                public int getCount() {
                    return count;
                }

                @Override
                public double getSum() {
                    return Double.longBitsToDouble(aggregates[index * 5]);
                }

                @Override
                public double getMinValue() {
                    return Double.longBitsToDouble(aggregates[index * 5 + 1]);
                }

                @Override
                public double getMaxValue() {
                    return Double.longBitsToDouble(aggregates[index * 5 + 3]);
                }

                @Override
                public double getAverageValue() {
                    return Double.longBitsToDouble(aggregates[index * 5]) / count;
                }

                @Override
                public long getMinTimestamp() {
                    return aggregates[index * 5 + 2];
                }

                @Override
                public long getMaxTimestamp() {
                    return aggregates[index * 5 + 4];
                }

                @Override
                public double getFirstValue() {
                    return (getMinValue() + getMaxValue()) / 2;
                }

                @Override
                public long getFirstTimestamp() {
                    return timestamp + 1;
                }

                @Override
                public double getLastValue() {
                    return (getMinValue() + getMaxValue()) / 2;
                }

                @Override
                public long getLastTimestamp() {
                    return getTo() - 1;
                }
            };
        }

        @Override
        public int getMeasure() {
            return measure;
        }

        @Override
        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public long getFrom() {
            return timestamp;
        }

        @Override
        public long getTo() {
            if (currentIndex == size - 1) {
                return to;
            } else {
                return from + (currentIndex + 1) * aggregateInterval;
            }
        }

        @Override
        public double getValue() {
            throw new UnsupportedOperationException();
        }
    }


}
