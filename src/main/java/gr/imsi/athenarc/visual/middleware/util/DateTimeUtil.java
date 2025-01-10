package gr.imsi.athenarc.visual.middleware.util;

import gr.imsi.athenarc.visual.middleware.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class DateTimeUtil {

    public static final ZoneId UTC = ZoneId.of("UTC");
    public final static String DEFAULT_FORMAT = "yyyy-MM-dd[ HH:mm:ss.SSS]";
    public final static DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_FORMAT);
    private static final Logger LOG = LoggerFactory.getLogger(DateTimeUtil.class);
    private static final  int[] millisDivisors = {1, 2, 4, 5, 8, 10, 20, 25, 40, 50, 100, 125, 200, 250, 500, 1000};


     public static long parseDateTimeString(String s, String timeFormat) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
        return parseDateTimeStringInternal(s, formatter, UTC);
    }

    public static long parseDateTimeString(String s, DateTimeFormatter formatter) {
        return parseDateTimeStringInternal(s, formatter, UTC);
    }

    public static long parseDateTimeString(String s, DateTimeFormatter formatter, ZoneId zoneId) {
        return parseDateTimeStringInternal(s, formatter, zoneId);
    }

    public static long parseDateTimeString(String s) {
        return parseDateTimeStringInternal(s, DEFAULT_FORMATTER, UTC);
    }

    private static long parseDateTimeStringInternal(String s, DateTimeFormatter formatter, ZoneId zoneId) {
        try {
            // Try parsing as LocalDateTime
            return LocalDateTime.parse(s, formatter).atZone(zoneId).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            // If parsing as LocalDateTime fails, try parsing as LocalDate
            return LocalDate.parse(s, formatter).atStartOfDay(zoneId).toInstant().toEpochMilli();
        }
    }

    public static String format(final long timeStamp) {
        return formatTimeStamp(DEFAULT_FORMATTER, timeStamp);
    }

    public static String format(final long timeStamp, final ZoneId zone) {
        return format(DEFAULT_FORMATTER, timeStamp, zone);
    }

    public static String format(final String format, final long timeStamp) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return Instant.ofEpochMilli(timeStamp)
                .atZone(UTC)
                .format(formatter);
    }

    public static String format(final String format, final long timeStamp, final ZoneId zone) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return Instant.ofEpochMilli(timeStamp)
                .atZone(zone)
                .format(formatter);
    }

    public static String format(final DateTimeFormatter formatter, final long timeStamp, final ZoneId zone) {
        return Instant.ofEpochMilli(timeStamp)
                .atZone(zone)
                .format(formatter);
    }

    public static String formatTimeStamp(final long timeStamp) {
        return formatTimeStamp(DEFAULT_FORMATTER, timeStamp);
    }

    public static String formatTimeStamp(final String format, final long timeStamp) {
        return format(format, timeStamp, UTC);
    }

    public static String formatTimeStamp(final DateTimeFormatter formatter, final long timeStamp) {
        return format(formatter, timeStamp, UTC);
    }
    public static ZonedDateTime getIntervalEnd(long timestamp, AggregateInterval aggregateInterval, ZoneId zoneId) {
        return getIntervalStart(timestamp, aggregateInterval, zoneId).plus(aggregateInterval.getInterval(), aggregateInterval.getChronoUnit());
    }

        /**
         * Returns the start date time of the interval that the timestamp belongs,
         * based on the given interval, unit and timezone.
         * It follows a calendar-based approach, considering intervals that align with the start of day,
         * month, etc.
         *
         * @param timestamp The timestamp to find an interval for, in milliseconds from epoch
         * @param aggregateInterval  The aggregate interval
         * @param zoneId    The zone id.
         * @return A ZonedDateTime instance set to the start of the interval this timestamp belongs
         * @throws IllegalArgumentException if the timestamp is negative or the interval is less than one
         */
    public static ZonedDateTime getIntervalStart(long timestamp, AggregateInterval aggregateInterval, ZoneId zoneId) {
        if (timestamp < 0) {
            throw new IllegalArgumentException("Timestamp cannot be less than zero");
        }
        if (aggregateInterval.getInterval() < 1) {
            throw new IllegalArgumentException("AggregateInterval interval must be greater than zero");
        }

        if (zoneId == null) {
            zoneId = ZoneId.of("UTC");
        }


        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);

        long interval = aggregateInterval.getInterval();
        ChronoUnit unit = aggregateInterval.getChronoUnit();

//        switch (aggregateInterval.getChronoUnit()) {
//            case MILLIS:
//                if (1000 % interval == 0) {
//                    dateTime = dateTime.withNano(0);
//                } else {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0);
//                }
//                break;
//            case SECONDS:
//                if (60 % interval == 0) {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0);
//                } else {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0).withMinute(0);
//                }
//                break;
//            case MINUTES:
//                if (60 % interval == 0) {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0).withMinute(0);
//                } else {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0).withMinute(0).withHour(0);
//                }
//                break;
//            case HOURS:
//                if (24 % interval == 0) {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0).withMinute(0).withHour(0);
//                } else {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0);
//                }
//                break;
//            case DAYS:
//                if (interval == 1) {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0);
//                } else {
//                    dateTime = dateTime.withNano(0)
//                            .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0).withMonth(0);
//                }
//                break;
//            case MONTHS:
//            case YEARS:
//                dateTime = dateTime.withNano(0)
//                        .withSecond(0).withMinute(0).withHour(0).withDayOfMonth(0).withMonth(0);
//                break;
//            default:
//                throw new IllegalArgumentException("Unexpected unit of type: "
//                        + unit);
//        }
//
//        if (dateTime.toInstant().toEpochMilli() == timestamp) {
//            return dateTime;
//        }
//        while (dateTime.toInstant().toEpochMilli() <= timestamp) {
//            dateTime = dateTime.plus(interval, unit);
//        }
//        dateTime = dateTime.minus(interval, unit);
        return dateTime;
    }

    public static int numberOfIntervals(final long startTime, final long endTime, long aggregateInterval) {
        return (int) Math.ceil((double)(endTime - startTime) / aggregateInterval);
    }

    public static int indexInInterval(final long startTime, final long endTime, final long aggregateInterval, final long time) {
        return (int) ((time - startTime ) / aggregateInterval);
    }
    /**
     * Returns the optimal M4 sampling interval in a specific range.
     * @param from The start timestamp to find the M4 sampling interval for (in timestamps)
     * @param to The end timestamp to find the M4 sampling interval for (in timestamps)
     * @param width  A viewport object that contains information about the chart that the user is visualizing
     * @return A Duration
     */
    public static Duration M4(long from, long to, int width) {
        long millisInRange = Duration.of(to - from, ChronoUnit.MILLIS).toMillis() / width;
        return Duration.of(millisInRange, ChronoUnit.MILLIS);
    }

    public static AggregateInterval M4Interval(long from, long to, int width){
        Duration duration = M4(from, to, width);
        return aggregateInterval(duration);
    }

    /**
     * Returns a sampling interval less than or equal to the given interval, so that it can exactly divide longer durations,
     * based on the gregorian calendar.
     * For this, we check how the given interval divides the next
     * calendar based frequency (e.g seconds -> minute, minute -> hour etc.) To get the closest exact division,
     * we floor the remainder (if it is decimal) and add 1 to it. Then, we divide the remaining number with how many
     * times the current frequency fits onto the next, to get the calendar based sampling interval that is closest to the given one.
     *
     * @param samplingInterval The sampling interval
     * @return A Duration
     */

    private static Duration maxCalendarInterval(Duration samplingInterval) {
        // Get each part that makes up a calendar date. The first non-zero is its "granularity".

        int days = (int) samplingInterval.toDaysPart();
        int hours = samplingInterval.toHoursPart();
        int minutes = samplingInterval.toMinutesPart();
        int seconds = samplingInterval.toSecondsPart();
        int millis = samplingInterval.toMillisPart();
        long t = 0L;
        if(days != 0){
            t = Duration.ofDays(30).toMillis();
        }
        else if(hours != 0){
            t = Duration.ofHours(24).toMillis();
        }
        else if(minutes != 0){
            t = Duration.ofMinutes(60).toMillis();
        }
        else if(seconds != 0){
            t = Duration.ofSeconds(60).toMillis();
        }
        else if(millis != 0){
            t = 1000;
        }
        double divisor = t * 1.0 / samplingInterval.toMillis();
        double flooredDivisor = Math.floor(divisor);
        divisor = flooredDivisor == divisor ? divisor : flooredDivisor + 1;
        Duration calendarInterval = Duration.of(t, ChronoUnit.MILLIS).dividedBy((long) divisor);
        double newMillis = calendarInterval.toMillis() / 1000F;
        if(newMillis >= 1) { // 1 second and above
            if (newMillis != (int) newMillis) // reached an exact division
                return maxCalendarInterval(Duration.of((long) (Math.floor(calendarInterval.toMillis() / 1000F) * 1000), ChronoUnit.MILLIS));
        }
        else { // 1 second and below
            int m = (int) (newMillis * 1000);
            return maxMillisInterval(m);
        }
        return calendarInterval;
    }


    public static Duration maxMillisInterval(int millis) {
        int i = 0;
        while (millis >= millisDivisors[i++]);
        return Duration.of(millisDivisors[--i], ChronoUnit.MILLIS);
    }

    public static AggregateInterval accurateInterval(long from, long to, ViewPort viewPort,
                                                     Duration samplingInterval, float accuracy) {
        Duration timeRangeDuration  = Duration.of(to - from, ChronoUnit.MILLIS);
        int partiallyOverlapped = viewPort.getWidth();
        Duration subDuration = Duration.ofMillis((long) (timeRangeDuration.toMillis() * (1 - accuracy) / (partiallyOverlapped + 1)) / 2);
        subDuration = subDuration.toMillis() < samplingInterval
                .toMillis() ? samplingInterval : subDuration;
        return aggregateInterval(subDuration);
    }

    public static Duration accurateCalendarInterval(long from, long to, ViewPort viewPort, float accuracy) {
        Duration timeRangeDuration  = Duration.of(to - from, ChronoUnit.MILLIS);
        int partiallyOverlapped = viewPort.getWidth();
        Duration accurateDuration = Duration.ofMillis((long) (timeRangeDuration.toMillis() * (1 - accuracy) / (partiallyOverlapped + 1)) / 2);
        return maxCalendarInterval(accurateDuration);
    }

    public static String getInfluxDBAggregateWindow(AggregateInterval aggregateInterval) {
        switch (aggregateInterval.getChronoUnit().toString()) {
            case ("Millis"):
                return aggregateInterval.getInterval() + "ms";
            case ("Seconds"):
                return aggregateInterval.getInterval() + "s";
            case ("Minutes"):
                return aggregateInterval.getInterval() + "m";
            case ("Hours"):
                return aggregateInterval.getInterval() + "h";
            default:
                return "inf";
        }
    }

    public static AggregateInterval aggregateInterval(Duration interval){
        long aggregateInterval = interval.toMillis();
        ChronoUnit aggregateChronoUnit = ChronoUnit.MILLIS;
        if(aggregateInterval % 1000 == 0){
            aggregateInterval = aggregateInterval / 1000;
            aggregateChronoUnit = ChronoUnit.SECONDS;
            if(aggregateInterval % 60 == 0){
                aggregateInterval = aggregateInterval / 60;
                aggregateChronoUnit = ChronoUnit.MINUTES;
                if(aggregateInterval % 60 == 0){
                    aggregateInterval = aggregateInterval / 60;
                    aggregateChronoUnit = ChronoUnit.HOURS;
                    if(aggregateInterval % 24 == 0){
                        aggregateInterval = aggregateInterval / 24;
                        aggregateChronoUnit = ChronoUnit.DAYS;
                    }
                }
            }
        }
        return new AggregateInterval(aggregateInterval, aggregateChronoUnit);
    }

    private static long[] findBinRange(long x, long a, long b, long bin_width){
        long bin_index = (x - a) / bin_width;
        long bin_start = a + bin_index * bin_width;
        long bin_end = bin_start + bin_width;
        if (bin_index == bin_width) {
            bin_end = b; // Set bin_end to the upper limit of the interval
        }
        return new long[] {bin_start, bin_end};
    }

    /**
     *
     * @param pixelColumnInterval interval of the pixel columns
     * @param ranges missing ranges to group
     * @return
     */
    public static List<TimeInterval> groupIntervals(long pixelColumnInterval, List<TimeInterval> ranges) {
        if(ranges.size() == 0) return ranges;
        List<TimeInterval> groupedRanges = new ArrayList<>();
        TimeInterval currentGroup = ranges.get(0);
        for(TimeInterval currentRange : ranges){
            if (currentGroup.getTo() + (pixelColumnInterval * 10) >= currentRange.getFrom() && groupedRanges.size() > 0) {
                // Extend the current group
                currentGroup = new TimeRange(currentGroup.getFrom(), currentRange.getTo());
                groupedRanges.set(groupedRanges.size() - 1, currentGroup);
            } else {
                // Start a new group
                currentGroup = currentRange;
                groupedRanges.add(currentGroup);
            }
        }
        return groupedRanges;
    }

    /**
     *
     * @param pixelColumnInterval interval of the pixel columns
     * @param ranges missing multivariate ranges to group. Each range contains a list of measures that it's missing.
     * @return
     */
    public static List<MultivariateTimeInterval> groupMultiIntervals(long pixelColumnInterval, List<MultivariateTimeInterval> ranges) {
        if (ranges.size() == 0) return ranges;

        List<MultivariateTimeInterval> groupedRanges = new ArrayList<>();
        MultivariateTimeInterval currentGroup = ranges.get(0);

        for (int i = 1; i < ranges.size(); i++) {
            MultivariateTimeInterval currentRange = ranges.get(i);

            if (haveSameMeasures(currentGroup, currentRange) && areContiguous(pixelColumnInterval, currentGroup.getInterval(), currentRange.getInterval())) {
                // Extend the current group
                currentGroup = mergeIntervals(currentGroup, currentRange);
            } else {
                // Start a new group
                groupedRanges.add(currentGroup);
                currentGroup = currentRange;
            }
        }

        // Add the last group
        groupedRanges.add(currentGroup);

        return groupedRanges;
    }

    private static boolean haveSameMeasures(MultivariateTimeInterval interval1, MultivariateTimeInterval interval2) {
        List<Integer> measures1 = interval1.getMeasures();
        List<Integer> measures2 = interval2.getMeasures();
        return measures1 != null && measures2 != null && measures1.equals(measures2);
    }

    private static boolean areContiguous(long pixelColumnInterval, TimeInterval interval1, TimeInterval interval2) {
        return interval1.getTo() + (pixelColumnInterval * 10) >= interval2.getFrom();
    }

    private static MultivariateTimeInterval mergeIntervals(MultivariateTimeInterval interval1, MultivariateTimeInterval interval2) {
        return new MultivariateTimeInterval(
                new TimeRange(interval1.getInterval().getFrom(), interval2.getInterval().getTo()),
                interval1.getMeasures()
        );
    }

    public static List<TimeInterval> correctIntervals(long from, long to, long pixelColumnInterval, List<TimeInterval> ranges) {
        if (ranges.size() == 0) return ranges;
        List<TimeInterval> correctRanges = new ArrayList<>();
        for (TimeInterval currentRange : ranges) {
            long correctFrom = findBinRange(currentRange.getFrom(), from, to, pixelColumnInterval)[0];
            long correctTo = findBinRange(currentRange.getTo(), from, to, pixelColumnInterval)[0];
            correctRanges.add(new TimeRange(correctFrom, correctTo));
        }
        return  correctRanges;
    }
}
