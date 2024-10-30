package gr.imsi.athenarc.visual.middleware.datasource;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

import java.util.*;

public class InfluxDBQuery extends DataSourceQuery {

    final String bucket;
    final String measurement;
    final String timeFormat;

    public InfluxDBQuery(String bucket, String measurement, String timeFormat, 
    long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasure, Map<String, Integer> numberOfGroups) {
        super(from, to, missingIntervalsPerMeasure, numberOfGroups);
        this.bucket = bucket;
        this.measurement = measurement;
        this.timeFormat = timeFormat;
    }


    public InfluxDBQuery(String bucket, String measurement, String timeFormat, long from, long to, Map<String, List<TimeInterval>> missingIntervalsPerMeasure) {
        super(from, to, missingIntervalsPerMeasure);
        this.bucket = bucket;
        this.measurement = measurement;
        this.timeFormat = timeFormat;
    }


    @Override
    public String getFromDate() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        return super.getFromDate(format);
    }

    @Override
    public String getToDate() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        return super.getToDate(format);
    }

    @Override
    public String minMaxQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s =
                "aggregate = (tables=<-, agg, name, aggregateInterval, offset) => tables" +
                        "\n" +
                        "|> aggregateWindow(every: aggregateInterval, createEmpty:true, offset: offset, fn: agg, timeSrc:\"_start\")" +
                        "\n";

        int i = 0;
        for (String measureName : missingIntervalsPerMeasure.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                s += "data_" + i + " = () => from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")\n";
                i++;
            }
        }
        s += "union(\n" +
                "    tables: [\n";
        i = 0;
        for (String measureName : missingIntervalsPerMeasure.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                long rangeOffset = range.getFrom() % aggregateIntervals.get(measureName);
                s += "data_" + i + "() |> aggregate(agg: max, name: \"data_" + i + "\", offset: " + rangeOffset + "ms," + "aggregateInterval:" +  aggregateIntervals.get(measureName) + "ms"+ "),\n" +
                      "data_" + i + "() |> aggregate(agg: min, name: \"data_" + i + "\", offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms"+ "),\n";
                i++;
            }
        }
        s+= "])\n";
        s +=
                "|> group(columns: [\"_field\", \"_start\", \"_stop\",])\n" +
                "|> sort(columns: [\"_time\"], desc: false)\n";
    return s;
    }


    @Override
    public String m4QuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s = "customAggregateWindow = (every, fn, column=\"_value\", timeSrc=\"_time\", timeDst=\"_time\", offset, tables=<-) =>\n" +
                "  tables\n" +
                "    |> window(every:every, offset: offset, createEmpty:true)\n" +
                "    |> fn(column:column)\n" +
                "    |> group()" +
                "\n" +
                "aggregate = (tables=<-, agg, name, aggregateInterval, offset) => tables" +
                "\n" +
                "|> customAggregateWindow(every: aggregateInterval, fn: agg, offset: offset)" +
                "\n";

        int i = 0;
        for (String measureName : missingIntervalsPerMeasure.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                s += "data_" + i + " = () => from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\""  + measureName + "\")\n";
                i++;
            }
        }
        s += "union(\n" +
                "    tables: [\n";

        i = 0;
        for (String measureName : missingIntervalsPerMeasure.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                long rangeOffset = range.getFrom() % aggregateIntervals.get(measureName);
                s += "data_" + i + "() |> aggregate(agg: first, name: \"first\", offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms" + "),\n" +
                        "data_" + i + "() |> aggregate(agg: max, name: \"max\", offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms" + "),\n" +
                        "data_" + i + "() |> aggregate(agg: min, name: \"min\", offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms" + "),\n" +
                        "data_" + i + "() |> aggregate(agg: last, name: \"last\", offset: " + rangeOffset + "ms," + "aggregateInterval:" + aggregateIntervals.get(measureName) + "ms"+ "),\n";
                i++;
            }
        }
        s += "])\n";
        s +=    "|> group(columns: [\"table\"])\n" +
                "|> sort(columns: [\"_time\"], desc: false)\n";
        return s;
    }


    @Override
    public String rawQuerySkeleton() {
        String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        String s = "";
        int i = 0;
        for (String measureName : missingIntervalsPerMeasure.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                s += "data_" + i + " = () => from(bucket:" + "\"" + bucket + "\"" + ") \n" +
                        "|> range(start:" + range.getFromDate(format) + ", stop:" + range.getToDate(format) + ")\n" +
                        "|> filter(fn: (r) => r[\"_measurement\"] ==" + "\"" + measurement + "\"" + ") \n" +
                        "|> filter(fn: (r) => r[\"_field\"] ==\"" + measureName + "\")" +
                        " \n";
                i++;
            }
        }
        s += "union(\n" +
                "    tables: [\n";
        for (String measureName : missingIntervalsPerMeasure.keySet()) {
            for(TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                s += "data_" + i + "(),\n";
            }
        }
        s+= "])\n ";
        s+= "|>pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";

        return s;
    }

    @Override
    public int getNoOfQueries() {
        return this.getMissingIntervalsPerMeasure().size() * this.getMissingIntervalsPerMeasure().values().stream().mapToInt(List::size).sum();

    }



}
