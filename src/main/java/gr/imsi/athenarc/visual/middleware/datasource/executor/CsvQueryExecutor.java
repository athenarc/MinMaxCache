package gr.imsi.athenarc.visual.middleware.datasource.executor;

import gr.imsi.athenarc.visual.middleware.cache.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.cache.query.QueryResults;
import gr.imsi.athenarc.visual.middleware.datasource.csv.CsvAggregateDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.csv.CsvAggregateDataPointsIteratorM4;
import gr.imsi.athenarc.visual.middleware.datasource.csv.CsvDataPointsIterator;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.CsvDataset;
import gr.imsi.athenarc.visual.middleware.datasource.query.CsvQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.domain.AggregatedDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.util.io.csv.CsvTimeSeriesRandomAccessReader;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import gr.imsi.athenarc.visual.middleware.domain.csv.TimeSeriesCsv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CsvQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CsvQueryExecutor.class);

    CsvDataset dataset;
    String table;
    String schema;
    private int timeColumnIndex;
    private DateTimeFormatter formatter;


    public CsvQueryExecutor() {
    }

    public CsvQueryExecutor(AbstractDataset dataset) throws IOException {
        this.dataset = (CsvDataset) dataset;
        this.schema = dataset.getSchema();
        this.table = dataset.getTableName();
        this.formatter = DateTimeFormatter.ofPattern(this.dataset.getTimeFormat());
    }

    @Override
    public Map<Integer, List<DataPoint>> execute(DataSourceQuery q, QueryMethod method) throws IOException {
        switch (method) {
            case M4:
                return executeM4Query(q);
            case RAW:
                return executeRawQuery(q);
            case MIN_MAX:
                return executeMinMaxQuery(q);
            default:
                throw new UnsupportedOperationException("Unsupported Query Method");
        }
    }

    /*
     * Executes a query on the csv. 
     * The query maps measures to missing intervals.
     * Due to the nature of CSVs, we first merge all required times from all measures.
     * We then return them sequentially.
     * The class that will traverse this list is responsible for correctly handling the measures.
     */
    public Iterable<String[]> executeCsvQuery(CsvQuery csvQuery) throws IOException {
        Map<String, List<TimeInterval>> missingIntervalsPerMeasure = csvQuery.getMissingIntervalsPerMeasure();
        Set<TimeInterval> intervalsToProcess = new HashSet<>();
        for (List<TimeInterval> intervals : missingIntervalsPerMeasure.values()) {
            intervalsToProcess.addAll(intervals);
        }
    
        // Merge intervals to avoid overlapping
        List<TimeInterval> mergedIntervals = mergeIntervals(intervalsToProcess);
        List<String[]> resultData = new ArrayList<>();
        // Iterate over each merged interval
        for (TimeInterval interval : mergedIntervals) {
            // Find files that overlap with the current interval

            List<TimeSeriesCsv> overlappingIntervals = StreamSupport.stream(
                                Spliterators.spliteratorUnknownSize(dataset.getFileTimeRangeTree().overlappers(interval), 0), false)
                        // Keep only spans with an aggregate interval that is half or less than the pixel column interval to ensure at least one fully contained in every pixel column that the span fully overlaps
                        // This way, each of the groups of the resulting spans will overlap at most two pixel columns.
                        .collect(Collectors.toList());

            for (TimeSeriesCsv overlappingCsvFile : overlappingIntervals) {
                LOG.debug("File range: {}, interval: {}", overlappingCsvFile.getTimeRange(), interval);
                // Initialize a reader for the relevant file
                CsvTimeSeriesRandomAccessReader reader = new CsvTimeSeriesRandomAccessReader(
                    overlappingCsvFile.getFilePath(), dataset.getTimeCol(), dataset.getDelimiter(),
                    dataset.getHasHeader(), DateTimeFormatter.ofPattern(dataset.getTimeFormat())
                );

                // Get data for the current interval
                List<String[]> fileData = reader.getDataInRange(Math.max(interval.getFrom(), overlappingCsvFile.getFrom()), Math.min(interval.getTo(), overlappingCsvFile.getTo()));
                resultData.addAll(fileData);
                reader.close();
            }
        }
        return resultData;
    }
    

    // Helper method to merge overlapping intervals
    private List<TimeInterval> mergeIntervals(Set<TimeInterval> intervals) {
        List<TimeInterval> intervalList = new ArrayList<>(intervals);
        intervalList.sort(Comparator.comparingLong(TimeInterval::getFrom));

        List<TimeInterval> mergedIntervals = new ArrayList<>();
        TimeInterval prev = null;
        for (TimeInterval interval : intervalList) {
            if (prev == null) {
                prev = interval;
            } else {
                if (interval.getFrom() <= prev.getTo()) {
                    // Overlapping intervals, merge
                    prev = new TimeRange(prev.getFrom(), Math.max(prev.getTo(), interval.getTo()));
                } else {
                    mergedIntervals.add(prev);
                    prev = interval;
                }
            }
        }
        if (prev != null) {
            mergedIntervals.add(prev);
        }
        return mergedIntervals;
    }

    @Override
    public Map<Integer, List<DataPoint>> execute(String s) throws IOException {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public Map<Integer, List<DataPoint>> executeM4Query(DataSourceQuery q) throws IOException {
        CsvQuery csvQuery = (CsvQuery) q;
        Iterable<String[]> csvDataPoints = executeCsvQuery(csvQuery);
        Map<String, Integer> measuresMap = csvQuery.getMissingIntervalsPerMeasure().entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey, // Key mapping is the measure name
                    entry -> Arrays.asList(dataset.getHeader()).indexOf(entry.getKey()), // Value is the value of the measure
                    (v1, v2) -> v1, // Merge function to keep the first value in case of key collision
                    LinkedHashMap::new // Specify LinkedHashMap to maintain insertion order
            ));
        CsvAggregateDataPointsIteratorM4 csvAggregateDataPointsIteratorM4 = 
        new CsvAggregateDataPointsIteratorM4(csvDataPoints, csvQuery.getMissingIntervalsPerMeasure(),
            measuresMap, csvQuery.getAggregateIntervals(), dataset.getTimeColumnIndex(), DateTimeFormatter.ofPattern(dataset.getTimeFormat()));
        return collectAggregateDatapoints(csvAggregateDataPointsIteratorM4);

    }


    @Override
    public Map<Integer, List<DataPoint>> executeMinMaxQuery(DataSourceQuery q) throws IOException {
        CsvQuery csvQuery = (CsvQuery) q;
        Iterable<String[]> csvDataPoints = executeCsvQuery(csvQuery);
        Map<String, Integer> measuresMap = csvQuery.getMissingIntervalsPerMeasure().entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey, // Key mapping is the measure name
                    entry -> Arrays.asList(dataset.getHeader()).indexOf(entry.getKey()), // Value is the value of the measure
                    (v1, v2) -> v1, // Merge function to keep the first value in case of key collision
                    LinkedHashMap::new // Specify LinkedHashMap to maintain insertion order
            ));
        CsvAggregateDataPointsIterator csvAggregateDataPointsIterator = 
        new CsvAggregateDataPointsIterator(csvDataPoints, csvQuery.getMissingIntervalsPerMeasure(),
            measuresMap, csvQuery.getAggregateIntervals(), dataset.getTimeColumnIndex(), DateTimeFormatter.ofPattern(dataset.getTimeFormat()));

       return collectAggregateDatapoints(csvAggregateDataPointsIterator);
    }

    @Override
    public Map<Integer, List<DataPoint>> executeRawQuery(DataSourceQuery q) throws IOException {
        CsvQuery csvQuery = (CsvQuery) q;
        Iterable<String[]> csvDataPoints = executeCsvQuery(csvQuery);
        Map<String, Integer> measuresMap = csvQuery.getMissingIntervalsPerMeasure().entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey, // Key mapping is the measure name
                    entry -> Arrays.asList(dataset.getHeader()).indexOf(entry.getKey()), // Value is the value of the measure
                    (v1, v2) -> v1, // Merge function to keep the first value in case of key collision
                    LinkedHashMap::new // Specify LinkedHashMap to maintain insertion order
            ));
        CsvDataPointsIterator csvDataPointsIterator = 
        new CsvDataPointsIterator(csvDataPoints, csvQuery.getMissingIntervalsPerMeasure(),
            measuresMap, dataset.getTimeColumnIndex(), DateTimeFormatter.ofPattern(dataset.getTimeFormat()));
        return collectDataPoints(csvDataPointsIterator);
    }

    @Override
    public void initialize(String path) throws NoSuchMethodException {
        throw new NoSuchMethodException("Unsupported method");
    }

    @Override
    public void drop() throws NoSuchMethodException {
        throw new NoSuchMethodException("Unsupported method");
    }

    Comparator<DataPoint> compareLists = new Comparator<DataPoint>() {
        @Override
        public int compare(DataPoint s1, DataPoint s2) {
            if (s1==null && s2==null) return 0; //swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };

    public String getTable() {
        return table;
    }

    public String getSchema() {
        return schema;
    }


    private Map<Integer, List<DataPoint>> collectAggregateDatapoints(Iterator<AggregatedDataPoint> iterator) {
        HashMap<Integer, List<DataPoint>> data = new HashMap<>();
        while(iterator.hasNext()){
            AggregatedDataPoint aggregatedDataPoint = iterator.next();
            Integer measure = aggregatedDataPoint.getMeasure();
            data.computeIfAbsent(measure, k -> new ArrayList<>()).add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getFirstTimestamp(), aggregatedDataPoint.getStats().getFirstValue(), aggregatedDataPoint.getMeasure()));
            data.computeIfAbsent(measure, k -> new ArrayList<>()).add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMinTimestamp(), aggregatedDataPoint.getStats().getMinValue(), aggregatedDataPoint.getMeasure()));
            data.computeIfAbsent(measure, k -> new ArrayList<>()).add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getMaxTimestamp(), aggregatedDataPoint.getStats().getMaxValue(), aggregatedDataPoint.getMeasure()));
            data.computeIfAbsent(measure, k -> new ArrayList<>()).add(new ImmutableDataPoint(aggregatedDataPoint.getStats().getLastTimestamp(), aggregatedDataPoint.getStats().getLastValue(), aggregatedDataPoint.getMeasure()));
        }
        
        data.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        return data;
    }

    private Map<Integer, List<DataPoint>> collectDataPoints(Iterator<DataPoint> iterator) {
        HashMap<Integer, List<DataPoint>> data = new HashMap<>();
        while(iterator.hasNext()){
            DataPoint dataPoint = iterator.next();
            Integer measure = dataPoint.getMeasure();
            data.computeIfAbsent(measure, k -> new ArrayList<>()).add(dataPoint);
        }
        data.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        return data;
    }

}

