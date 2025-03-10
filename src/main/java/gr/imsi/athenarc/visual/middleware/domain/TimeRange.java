package gr.imsi.athenarc.visual.middleware.domain;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * An implementation of the TimeInterval interface.
 */
public class TimeRange implements Serializable, TimeInterval {

    private long from;
    private long to;

    public TimeRange() {}

    public TimeRange(final long from, final long to) {
        this.from = from;
        this.to = to;
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
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getToDate() {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public String getFromDate(String format) {
        return Instant.ofEpochMilli(from).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    @Override
    public String getToDate(String format) {
        return Instant.ofEpochMilli(to).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
    }

    public boolean contains(TimeInterval other) {
        return (from <= other.getFrom() && to >= other.getTo());
    }

    public boolean intersects(TimeInterval other) {
        return (from < other.getTo() && to > other.getFrom());
    }

    public boolean encloses(TimeInterval other) {
        return (this.from < other.getFrom() && to > other.getTo());
    }

    public TimeRange span(TimeInterval other) {
        long fromCmp = from - other.getFrom();
        long toCmp = to - other.getTo();
        if (fromCmp <= 0 && toCmp >= 0) {
            return this;
        } else if (fromCmp >= 0 && toCmp <= 0) {
            return new TimeRange(other.getFrom(), other.getTo());
        } else {
            long newFrom = (fromCmp <= 0) ? from : other.getFrom();
            long newTo = (toCmp >= 0) ? to : other.getTo();
            return new TimeRange(newFrom, newTo);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeRange range = (TimeRange) o;
        return Objects.equals(from, range.from) &&
                Objects.equals(to, range.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    @Override
    public String toString() {
        return "[" + getFromDate() + "(" + getFrom() + ")"+
                ", " + getToDate() + "(" + getTo() + ")" +
                ')';
    }
}