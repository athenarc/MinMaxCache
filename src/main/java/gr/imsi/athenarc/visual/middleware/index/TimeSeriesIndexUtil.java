package gr.imsi.athenarc.visual.middleware.index;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoField.*;

public class TimeSeriesIndexUtil {
    public static final List<TemporalField> TEMPORAL_HIERARCHY = Arrays.asList(YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE, MILLI_OF_SECOND);
    private static BiMap<String, TemporalField> temporalFieldMap = HashBiMap.create();
    private static final Logger LOG = LogManager.getLogger(TimeSeriesIndexUtil.class);

    static {
        temporalFieldMap.put("YEAR", YEAR);
        temporalFieldMap.put("MONTH", MONTH_OF_YEAR);
        temporalFieldMap.put("DAY", DAY_OF_MONTH);
        temporalFieldMap.put("HOUR", HOUR_OF_DAY);
        temporalFieldMap.put("MINUTE", MINUTE_OF_HOUR);
        temporalFieldMap.put("SECOND", SECOND_OF_MINUTE);
        temporalFieldMap.put("MILLI", MILLI_OF_SECOND);
    }

    public static TemporalField getTemporalFieldByName(String name) {
        return temporalFieldMap.get(name);
    }

    public static String getTemporalFieldName(TemporalField temporalField) {
        return temporalFieldMap.inverse().get(temporalField);
    }

    public static int getTemporalLevelIndex(String temporalLevel) {
        return TEMPORAL_HIERARCHY.indexOf(getTemporalFieldByName(temporalLevel));
    }

    public static LocalDateTime truncate(LocalDateTime dateTime, TemporalField temporalField) {
        return dateTime.truncatedTo(temporalField.getBaseUnit());
    }

    public static LocalDateTime truncate(LocalDateTime dateTime, String unit) {
        return dateTime.truncatedTo(getTemporalFieldByName(unit).getBaseUnit());
    }


    public static LocalDateTime getLocalDateTimeFromTimestamp(long timestampInMillis){
        Instant instant = Instant.ofEpochMilli(timestampInMillis);
        return instant.atZone(ZoneId.of("UTC")).toLocalDateTime();
    }

    public static long getTimestampFromLocalDateTime(LocalDateTime localDateTime) {
        // Convert LocalDateTime to an Instant
        Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
        // Get the timestamp in milliseconds
        return instant.toEpochMilli();
    }

   public static String calculateFreqLevel(long from, long to){
       long differenceInMillis = to - from;
       if (differenceInMillis <= TimeUnit.MINUTES.toMillis(60)) {
           LOG.debug("SECOND");
           return "SECOND";
       } else if (differenceInMillis <= 24 * TimeUnit.HOURS.toMillis(60)) {
           LOG.debug("MINUTE");
           return "MINUTE";
       } else if (differenceInMillis <= 24 * 30 * TimeUnit.HOURS.toMillis(1)) {
           LOG.debug("HOUR");

           return "HOUR";
       } else {
           // Handle the case where the difference is more than an hour
           return "HOUR";
       }
   }


}

