
package gr.imsi.athenarc.visual.middleware.datasource;

import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.m4.InfluxDBM4DataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.minmax.InfluxDBMinMaxDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.raw.InfluxDBDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.iterator.slope.InfluxDBSlopeDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.query.InfluxDBQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.domain.*;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class InfluxDBDatasource implements DataSource {

    InfluxDBQueryExecutor influxDBQueryExecutor;
    InfluxDBDataset dataset;
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBDatasource.class);

    public InfluxDBDatasource(InfluxDBQueryExecutor influxDBQueryExecutor, InfluxDBDataset dataset) {
        this.dataset = dataset;
        this.influxDBQueryExecutor = influxDBQueryExecutor;
    }
    
    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(measures.size());
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(from, to));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new InfluxDBDatasource.InfluxDBDatapoints(from, to, missingTimeIntervalsPerMeasure);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure) {
        return new InfluxDBDatasource.InfluxDBDatapoints(from, to, missingTimeIntervalsPerMeasure);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(measures.size());
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo()));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new InfluxDBDatasource.InfluxDBDatapoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), missingTimeIntervalsPerMeasure);
    }

    @Override
    public AggregatedDataPoints getMinMaxDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                        Map<Integer, Integer> numberOfGroups) {
        return new InfluxDBDatasource.InfluxDBAggregateDatapoints(from, to, missingIntervalsPerMeasure, numberOfGroups, QueryMethod.MIN_MAX);
    }

    @Override
    public AggregatedDataPoints getM4DataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                        Map<Integer, Integer> numberOfGroups) {
        return new InfluxDBDatasource.InfluxDBAggregateDatapoints(from, to, missingIntervalsPerMeasure, numberOfGroups, QueryMethod.M4);
    }

    final class InfluxDBDatapoints implements DataPoints {
        private final InfluxDBQuery influxDBQuery;
        private final Map<String, Integer> measuresMap;

        public InfluxDBDatapoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
            Map<String, List<TimeInterval>> missingIntervalsPerMeasureName = missingIntervalsPerMeasure.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getValue // Value remains the same
                    ));

            this.measuresMap = missingIntervalsPerMeasure.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getKey, // Value is the key of the measure
                            (v1, v2) -> v1, // Merge function to keep the first value in case of key collision
                            LinkedHashMap::new // Specify LinkedHashMap to maintain insertion order
                    ));

            this.influxDBQuery = new InfluxDBQuery(dataset.getSchema(), dataset.getTableName(), dataset.getTimeFormat(), from, to, missingIntervalsPerMeasureName);
        }

        @Override
        public long getFrom() {
            return influxDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return influxDBQuery.getTo();
        }

        @NotNull
        @Override
        public Iterator<DataPoint> iterator() {
            try {
                List<FluxTable> fluxTables;
                fluxTables = influxDBQueryExecutor.executeRawInfluxQuery(influxDBQuery);
                return new InfluxDBDataPointsIterator(fluxTables, measuresMap, influxDBQuery.getMissingIntervalsPerMeasure());
            } catch (Exception e){
                LOG.error("No data in a specified query");
            }
            return Collections.emptyIterator();
        }
    }

    final class InfluxDBAggregateDatapoints implements AggregatedDataPoints {

        private final InfluxDBQuery influxDBQuery;
        private final Map<String, Integer> measuresMap;
        private final QueryMethod queryMethod;

        public InfluxDBAggregateDatapoints(long from, long to,
                                            Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                            Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod) {
            Map<String, List<TimeInterval>> missingIntervalsPerMeasureName = missingIntervalsPerMeasure.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getValue, // Value remains the same
                            (v1, v2) -> v1, // Merge function to keep the first value in case of key collision
                            LinkedHashMap::new // Specify LinkedHashMap to maintain insertion order
                    ));
            Map<String, Integer> numberOfGroupsPerMeasureName = numberOfGroups.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getValue, // Value remains the same
                            (v1, v2) -> v1, // Merge function to keep the first value in case of key collision
                            LinkedHashMap::new // Specify LinkedHashMap to maintain insertion order
                    ));
            this.measuresMap = missingIntervalsPerMeasure.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getKey, // Value is the key of the measure
                            (v1, v2) -> v1, // Merge function to keep the first value in case of key collision
                            LinkedHashMap::new // Specify LinkedHashMap to maintain insertion order
                    ));
            this.influxDBQuery = new InfluxDBQuery(dataset.getSchema(), dataset.getTableName(), dataset.getTimeFormat(), from, to,  missingIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
            this.queryMethod = queryMethod;
        }

        @NotNull
        @Override
        public Iterator<AggregatedDataPoint> iterator() {
            if(queryMethod == QueryMethod.M4){
                List<FluxTable> fluxTables = influxDBQueryExecutor.executeM4InfluxQuery(influxDBQuery);
                if (fluxTables.size() == 0) return Collections.emptyIterator();
                return new InfluxDBM4DataPointsIterator(fluxTables, measuresMap);
            }
            else if(queryMethod == QueryMethod.MIN_MAX){
                List<FluxTable> fluxTables = influxDBQueryExecutor.executeMinMaxInfluxQuery(influxDBQuery);
                if (fluxTables.size() == 0) return Collections.emptyIterator();
                return new InfluxDBMinMaxDataPointsIterator(fluxTables, measuresMap);
            }
            return Collections.emptyIterator();
        }

        @Override
        public long getFrom() {
            return influxDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return influxDBQuery.getTo();
        }

        @Override
        public String getFromDate() {
            return influxDBQuery.getFromDate();
        }

        @Override
        public String getToDate() {
            return influxDBQuery.getToDate();
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(influxDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(influxDBQuery.getTo()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));

        }
    }

    @Override
    public AbstractDataset getDataset() {
        return dataset;
    }

    public void closeConnection(){
        influxDBQueryExecutor.closeConnection();
    }

}