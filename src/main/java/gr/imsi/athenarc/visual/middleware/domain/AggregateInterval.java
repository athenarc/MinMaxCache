package gr.imsi.athenarc.visual.middleware.domain;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class AggregateInterval implements Comparable<AggregateInterval> {
    private long interval;
    private ChronoUnit chronoUnit;

    public AggregateInterval(long interval, ChronoUnit chronoUnit) {
        this.interval = interval;
        this.chronoUnit = chronoUnit;
    }

    public long getInterval() {
        return interval;
    }

    public ChronoUnit getChronoUnit() {
        return chronoUnit;
    }

    public Duration toDuration() {
        return Duration.of(interval, chronoUnit);
    }

    @Override
    public String toString() {
        return "AggregateInterval{" +
                interval +
                " " +
                chronoUnit +
                '}';
    }

    @Override
    public int compareTo(@NotNull AggregateInterval o) {
        return Long.compare(this.toDuration().toMillis(), o.toDuration().toMillis());
    }

}