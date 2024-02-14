package gr.imsi.athenarc.visual.middleware.datasource;

import com.google.common.collect.Iterators;
import gr.imsi.athenarc.visual.middleware.datasource.ModelarDB.ModelarDBAggregateDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.ModelarDB.ModelarDBDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.ModelarDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.ModelarDBDataset;
import gr.imsi.athenarc.visual.middleware.domain.Query.QueryMethod;
import cfjd.org.apache.arrow.flight.FlightStream;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class ModelarDBDatasource implements DataSource {

    ModelarDBQueryExecutor queryExecutor;
    ModelarDBDataset dataset;

    public ModelarDBDatasource(ModelarDBQueryExecutor queryExecutor, ModelarDBDataset dataset) {
        this.dataset = dataset;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                                        Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod) {
        return new ModelarDBAggregatedDataPoints(from, to, missingIntervalsPerMeasure, numberOfGroups, queryMethod);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(measures.size());
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(from, to));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new ModelarDBDatasource.ModelarDBDataPoints(from, to, missingTimeIntervalsPerMeasure);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        return new ModelarDBDatasource.ModelarDBDataPoints(from, to, missingIntervalsPerMeasure);
    }

    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(measures.size());
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo()));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new ModelarDBDatasource.ModelarDBDataPoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), missingTimeIntervalsPerMeasure);
    }

    /**
     * Represents a series of {@link ModelarDBDataPoints} instances.
     * The iterator returned from this class accesses the SQL database to request the data points.
     */
    final class ModelarDBDataPoints implements DataPoints {

        private final ModelarDBQuery modelarDBQuery;

        public ModelarDBDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
            Map<String, List<TimeInterval>> missingIntervalsPerMeasureName = missingIntervalsPerMeasure.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getValue // Value remains the same
                    ));
            this.modelarDBQuery = new ModelarDBQuery(from, to, missingIntervalsPerMeasureName);
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            try {
                FlightStream flightStream = queryExecutor.executeRawModelarDBQuery(modelarDBQuery);
                return new ModelarDBDataPointsIterator(null,
                        dataset.getTimeCol(), dataset.getValueCol(), dataset.getIdCol(), flightStream);
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public String toString() {
            return "ModelarDBDataPoints{" +
                    "missingIntervalsPerMeasure=" + modelarDBQuery.getMissingIntervalsPerMeasure() +
                    ", from=" + modelarDBQuery.getFrom() +
                    ", to=" + modelarDBQuery.getTo() +
                    '}';
        }

        @Override
        public long getFrom() {
            return modelarDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return modelarDBQuery.getTo();
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
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }
    }

    final class ModelarDBAggregatedDataPoints implements AggregatedDataPoints {

        private final ModelarDBQuery modelarDBQuery;

        private final QueryMethod queryMethod;


        public ModelarDBAggregatedDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure,
                                        Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod) {
            Map<String, List<TimeInterval>> missingIntervalsPerMeasureName = missingIntervalsPerMeasure.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getValue // Value remains the same
                    ));
            Map<String, Integer> numberOfGroupsPerMeasureName = numberOfGroups.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> dataset.getHeader()[entry.getKey()], // Key mapping is the measure name
                            Map.Entry::getValue // Value remains the same
                    ));
            this.modelarDBQuery = new ModelarDBQuery(from, to, missingIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
            this.queryMethod = queryMethod;
        }

        @NotNull
        public Iterator<AggregatedDataPoint> iterator() {
            try {
                FlightStream flightStream = queryExecutor.executeMinMaxModelarDBQuery(modelarDBQuery);
                return new ModelarDBAggregateDataPointsIterator(modelarDBQuery.getFrom(), modelarDBQuery.getTo(),
                        new ArrayList<>(), dataset.getTimeCol(), dataset.getValueCol(), dataset.getIdCol(), flightStream, null);
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public String toString() {
            return "PostgreSQLDataPoints{" +
                    "missingINtervalsPerMeasure=" + modelarDBQuery.getMissingIntervalsPerMeasure() +
                    ", from=" + modelarDBQuery.getFrom() +
                    ", to=" + modelarDBQuery.getTo() +
                    '}';
        }

        @Override
        public long getFrom() {
            return modelarDBQuery.getFrom();
        }

        @Override
        public long getTo() {
            return modelarDBQuery.getTo();
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
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(modelarDBQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }
    }
}
