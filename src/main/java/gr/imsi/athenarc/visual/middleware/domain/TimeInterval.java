package gr.imsi.athenarc.visual.middleware.domain;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import gr.imsi.athenarc.visual.middleware.cache.TimeSeriesSpan;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Closed-open, [), interval on the integer number line.
 */
public interface TimeInterval extends Comparable<TimeInterval> {

    /**
     * Returns the starting point of this interval
     */
    long getFrom();

    /**
     * Returns the ending point of this interval
     * <p>
     * The interval does not include this point.
     */
    long getTo();

    default String getFromDate(String format) {
        return Instant.ofEpochMilli(getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    default String getFromDate() {
        return getFromDate("yyyy-MM-dd HH:mm:ss");
    }

    default String getToDate() {
        return getToDate("yyyy-MM-dd HH:mm:ss");
    }

    default String getToDate(String format) {
        return Instant.ofEpochMilli(getTo()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    default long length() {
        return getTo() - getFrom();
    }


    default boolean contains(long x) {
        return getFrom() <= x && getTo() > x;
    }

    /**
     * Returns if this interval is adjacent to the specified interval.
     * <p>
     * Two intervals are adjacent if either one ends where the other starts.
     *
     * @param other - the interval to compare this one to
     * @return if this interval is adjacent to the specified interval.
     */
    default boolean isAdjacent(TimeInterval other) {
        return getFrom() == other.getTo() || getTo() == other.getFrom();
    }

    default boolean overlaps(TimeInterval other) {
        return getTo() > other.getFrom() && other.getTo() > getFrom();
    }


    default boolean encloses(TimeInterval other) {
        return (getFrom() <= (other.getFrom()) && this.getTo() >= (other.getTo()));
    }


    default double percentage(TimeInterval other) {
        return 1.0 - ((float) (Math.max(other.getTo() - getTo(), 0) + Math.max(getFrom() - other.getFrom(), 0))) / (other.getTo() - other.getFrom());
    }


    default int compareTo(TimeInterval o) {
        if (getFrom() > o.getFrom()) {
            return 1;
        } else if (getFrom() < o.getFrom()) {
            return -1;
        } else if (getTo() > o.getTo()) {
            return 1;
        } else if (getTo() < o.getTo()) {
            return -1;
        } else {
            return 0;
        }
    }

    // Get a Guava Range from a time interval
    public static Range<Long> toGuavaRange(TimeInterval timeInterval) {
        return Range.closedOpen(timeInterval.getFrom(), timeInterval.getTo());
    }

    /**
     * Calculate the difference between this TimeInterval and a list of TimeIntervals
     *
     * @param others - list of TimeIntervals
     * @return a list of TimeIntervals representing ranges in this TimeInterval not covered by any TimeInterval in others
     */
    default List<TimeInterval> difference(List<TimeSeriesSpan> others) {
        Range<Long> thisRange = toGuavaRange(this);
        RangeSet<Long> rangeSet = TreeRangeSet.create();
        others.stream()
                .map(TimeInterval::toGuavaRange)
                .forEach(rangeSet::add);

        ImmutableRangeSet<Long> otherRangeSet = ImmutableRangeSet.copyOf(rangeSet);

        return ImmutableRangeSet.of(thisRange).difference(otherRangeSet).asRanges().stream()
                .map(r -> new TimeRange(r.lowerEndpoint(), r.upperEndpoint()))
                .collect(Collectors.toList());
    }

    default String getIntervalString() {
        return "[" + Instant.ofEpochMilli(getFrom()) + " (" + getFrom() + "), " + Instant.ofEpochMilli(getTo()) + " (" + getTo() + "))";
    }

}
