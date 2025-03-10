package gr.imsi.athenarc.visual.middleware.datasource.query;

import java.util.*;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

public class InfluxDBQuery extends DataSourceQuery {

    final String bucket;
    final String measurement;
    final String timeFormat;
    final String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

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
        return super.getFromDate(format);
    }

    @Override
    public String getToDate() {
        return super.getToDate(format);
    }

    @Override
    public String minMaxQuerySkeleton() {
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
    public String rawQuerySkeleton() {
        StringBuilder s = new StringBuilder();
        int i = 0;
        int streamCount = 0;

        for (String measureName : missingIntervalsPerMeasure.keySet()) {
            for (TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                s.append("data_").append(i).append(" = () => from(bucket: \"").append(bucket).append("\") \n")
                    .append("|> range(start: ").append(range.getFromDate(format)).append(", stop: ").append(range.getToDate(format)).append(")\n")
                    .append("|> filter(fn: (r) => r[\"_measurement\"] == \"").append(measurement).append("\") \n")
                    .append("|> filter(fn: (r) => r[\"_field\"] == \"").append(measureName).append("\") \n");
                i++;
                streamCount++;
            }
        }

        if (streamCount > 1) {
            s.append("union(\n")
                .append("    tables: [\n");
            i = 0;
            for (String measureName : missingIntervalsPerMeasure.keySet()) {
                for (TimeInterval range : missingIntervalsPerMeasure.get(measureName)) {
                    s.append("data_").append(i).append("(),\n");
                    i++;
                }
            }
            s.append("])\n");
        } else if (streamCount == 1) {
            s.append("data_0()\n");
        } else {
            // Handle the case where there are no streams
            return "";
        }

        s.append("|> group(columns: [\"_field\"])\n" +
                "|> sort(columns: [\"_time\"], desc: false)\n");

        return s.toString();
    }


    @Override
    public String m4QuerySkeleton() {
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
        s +=    "|> group(columns: [\"_field\"])\n" +
                "|> sort(columns: [\"_time\"], desc: false)\n";
        return s;
    }


    @Override
    public int getNoOfQueries() {
        return this.getMissingIntervalsPerMeasure().size() * this.getMissingIntervalsPerMeasure().values().stream().mapToInt(List::size).sum();
    }

}
