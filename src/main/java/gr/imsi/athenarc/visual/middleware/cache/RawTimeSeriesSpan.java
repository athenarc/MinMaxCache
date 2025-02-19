package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * A {@link DataPoints} implementation that stores  a series of consecutive
 * raw data points.
 */
public class RawTimeSeriesSpan implements TimeSeriesSpan {
    private static final Logger LOG = LoggerFactory.getLogger(RawTimeSeriesSpan.class);

    int count = 0;
    /**
     * The raw datapoint values.
     */
    private double[] values;

    /**
     * The timestamps of the raw datapoints.
     */
    private long[] timestamps;

    // The start time value of the span
    private long from;

    // The end time value of the span
    // Keep in mind that the end time is not included in the span,
    private long to;

    /*
     The measure for which this span is for.
     */
    private int measure;

    protected RawTimeSeriesSpan(long from, long to, int measure) {
        this.from = from;
        this.to = to;
        this.measure = measure;
    }

    /**
     * @param dataPoints
     */


     protected void build(List<DataPoint> dataPoints) {
        ArrayList<Double> valuesList = new ArrayList<>();
        ArrayList<Long> timestampsList = new ArrayList<>();
        for (DataPoint dataPoint : dataPoints) {
            timestampsList.add(dataPoint.getTimestamp());
            valuesList.add(dataPoint.getValue());
            count ++;
        }
        values = valuesList.stream().mapToDouble(Double::doubleValue).toArray();
        timestamps = timestampsList.stream().mapToLong(Long::longValue).toArray();
    }


    /**
     * Finds the index in the span in which the given timestamp should be.
     *
     * @param timestamp A timestamp in milliseconds since epoch.
     * @return A positive index.
     */
    private int getIndex(final long timestamp) {
        int index = Arrays.binarySearch(timestamps, timestamp);
        if (index < 0) {
            // If not exact match, convert negative index to insertion point
            index = -(index + 1);
        }
        return index;
    }

    public TimeRange getTimeRange() {
        return new TimeRange(getFrom(), getTo());
    }

    @Override
    public long getAggregateInterval() {
        return -1;
    }

    public Iterator<DataPoint> iterator(long queryStartTimestamp, long queryEndTimestamp) {
        return new RawTimeSeriesSpanIterator(queryStartTimestamp, queryEndTimestamp);
    }

    @Override
    public Iterator<DataPoint> iterator() {
        // Use the first and last timestamps as the range for the iterator
        return new RawTimeSeriesSpanIterator(from, to);
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
    public String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(getFrom()).atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(getTo()).atZone(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String toString() {
        return getFromDate() + " - " + getToDate() + " for measure: " + measure;
    }

    @Override
    public int getCount() {
        return count;
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
        // Memory usage of double in a 64-bit JVM
        final int DOUBLE_SIZE = 8;
        // Memory usage of a reference in a 64-bit JVM with a heap size less than 32 GB
        final int REF_SIZE = 4;


        long valuesByMeasureMemory = REF_SIZE + ARRAY_OVERHEAD + (values.length * DOUBLE_SIZE);

        long timestampsMemory = REF_SIZE + ARRAY_OVERHEAD + (timestamps.length * LONG_SIZE);

        long deepMemorySize = REF_SIZE + OBJECT_OVERHEAD  +
                valuesByMeasureMemory + timestampsMemory;

        return deepMemorySize;
    }

    @Override
    public int getMeasure() {
        return measure;
    }

    private class RawTimeSeriesSpanIterator implements Iterator<DataPoint> {
        private int startIndex;
        private int endIndex;
        private int currentIndex;

        public RawTimeSeriesSpanIterator(long queryStartTimestamp, long queryEndTimestamp) {
            startIndex = getIndex(queryStartTimestamp);
            endIndex = getIndex(queryEndTimestamp);
            currentIndex = startIndex;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < endIndex;
        }

        @Override
        public DataPoint next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final long timestamp = timestamps[currentIndex];
            final double value = values[currentIndex];
            currentIndex++;

            return new DataPoint() {
                @Override
                public long getTimestamp() {
                    return timestamp;
                }

                @Override
                public double getValue() {
                    return value;
                }

                @Override
                public int getMeasure() {
                    return measure;
                }
            };
        }
    }
}
