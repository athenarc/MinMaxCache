package gr.imsi.athenarc.visual.middleware.datasource;
import com.google.common.collect.Iterators;

import gr.imsi.athenarc.visual.middleware.domain.*;
import gr.imsi.athenarc.visual.middleware.cache.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.datasource.csv.CsvAggregateDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.csv.CsvAggregateDataPointsIteratorM4;
import gr.imsi.athenarc.visual.middleware.datasource.csv.CsvDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.CsvDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.CsvQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.query.CsvQuery;

import org.checkerframework.checker.units.qual.C;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class CsvDatasource implements DataSource {

    CsvQueryExecutor csvQueryExecutor;
    CsvDataset dataset;

    public CsvDatasource(CsvQueryExecutor csvQueryExecutor, CsvDataset dataset) {
        this.dataset = dataset;
        this.csvQueryExecutor = csvQueryExecutor;
    }

    @Override
    public AggregatedDataPoints getAggregatedDataPoints(long from, long to,
                                                        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod) {
        return new CsvAggregatedDataPoints(from, to, missingIntervalsPerMeasure, numberOfGroups, queryMethod);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure = new HashMap<>();
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(from, to));
            missingIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new CsvDatasource.CsvDataPoints(from, to, missingIntervalsPerMeasure);
    }

    @Override
    public DataPoints getDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
        return new CsvDatasource.CsvDataPoints(from, to, missingIntervalsPerMeasure);
    }


    @Override
    public DataPoints getAllDataPoints(List<Integer> measures) {
        Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(measures.size());
        for (Integer measure : measures) {
            List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
            timeIntervalsForMeasure.add(new TimeRange(dataset.getTimeRange().getFrom(), dataset.getTimeRange().getTo()));
            missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
        }
        return new CsvDatasource.CsvDataPoints(dataset.getTimeRange().getFrom(),
                dataset.getTimeRange().getTo(), missingTimeIntervalsPerMeasure);
    }

    /**
     * Represents a series of {@link CsvDataPoints} instances.
     * The iterator returned from this class accesses the csv files to request the data points.
     */

    public class CsvDataPoints implements DataPoints {

        private final CsvQuery csvQuery;
        private final Map<String, Integer> measuresMap;

        public CsvDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure) {
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
            this.csvQuery = new CsvQuery(from, to, missingIntervalsPerMeasureName);
        }

        @NotNull
        public Iterator<DataPoint> iterator() {
            try {
                Iterable<String[]> csvDataPoints = csvQueryExecutor.executeCsvQuery(csvQuery);
                return new CsvDataPointsIterator(csvDataPoints, csvQuery.getMissingIntervalsPerMeasure(), measuresMap, dataset.getTimeColumnIndex(), DateTimeFormatter.ofPattern(dataset.getTimeFormat()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public long getFrom() {
            return csvQuery.getFrom();
        }

        @Override
        public long getTo() {
            return csvQuery.getFrom();
        }
        @Override
        public String getFromDate() {
            return getFromDate(dataset.getTimeFormat());
        }

        @Override
        public String getToDate() {
            return getToDate(dataset.getTimeFormat());
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(csvQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(csvQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

    }

    final class CsvAggregatedDataPoints implements AggregatedDataPoints {

        private final CsvQuery csvQuery;
        private final QueryMethod queryMethod;
        private final Map<String, Integer> measuresMap;


        public CsvAggregatedDataPoints(long from, long to, Map<Integer, List<TimeInterval>> missingIntervalsPerMeasure, Map<Integer, Integer> numberOfGroups, QueryMethod queryMethod) {
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
            this.csvQuery = new CsvQuery(from, to, missingIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
            this.queryMethod = queryMethod;
        }

        @NotNull
        public Iterator<AggregatedDataPoint> iterator() {
            try {
                Iterable<String[]> csvDataPoints = csvQueryExecutor.executeCsvQuery(csvQuery);
                if (queryMethod == QueryMethod.M4) {
                    return new CsvAggregateDataPointsIteratorM4(csvDataPoints, csvQuery.getMissingIntervalsPerMeasure(),
                    measuresMap, csvQuery.getAggregateIntervals(), dataset.getTimeColumnIndex(), DateTimeFormatter.ofPattern(dataset.getTimeFormat()));
                } else {
                    return new CsvAggregateDataPointsIterator(csvDataPoints, csvQuery.getMissingIntervalsPerMeasure(),
                    measuresMap, csvQuery.getAggregateIntervals(), dataset.getTimeColumnIndex(), DateTimeFormatter.ofPattern(dataset.getTimeFormat()));
        
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return Iterators.concat(new Iterator[0]);
        }

        @Override
        public String toString() {
            return "CsvDataPoints{" +
                    "measures=" + csvQuery.getMissingIntervalsPerMeasure().keySet() +
                    ", from=" + csvQuery.getFrom() +
                    ", to=" + csvQuery.getTo() +
                    '}';
        }

        @Override
        public long getFrom() {
            return csvQuery.getFrom();
        }

        @Override
        public long getTo() {
            return csvQuery.getTo();
        }

        @Override
        public String getFromDate() {
            return getFromDate(dataset.getTimeFormat());
        }

        @Override
        public String getToDate() {
            return getToDate(dataset.getTimeFormat());
        }

        @Override
        public String getFromDate(String format) {
            return Instant.ofEpochMilli(csvQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }

        @Override
        public String getToDate(String format) {
            return Instant.ofEpochMilli(csvQuery.getFrom()).atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern(format));
        }
    }

 

}
