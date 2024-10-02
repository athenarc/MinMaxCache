package gr.imsi.athenarc.visual.middleware.index.csv;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.CsvDataset;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import gr.imsi.athenarc.visual.middleware.index.TimeSeriesIndexUtil;
import gr.imsi.athenarc.visual.middleware.index.TreeNode;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.TemporalField;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvQueryProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CsvQueryProcessor.class);
    private Query query;
    private QueryResults queryResults;
    private CsvDataset dataset;
    private int freqLevel;
    private Stack<TreeNode> stack = new Stack<>();
    private FileInputStream fileInputStream;
    private CsvParser parser;
    private CsvTTI tti;
    private List<Integer> measures;
    private HashMap<Integer, Double[]> filter;
    private Integer accumulatorCounter;


    public CsvQueryProcessor(Query query, CsvDataset dataset, CsvTTI tti) {
        this.query = query;
        this.filter = query.getFilter();
        this.measures = query.getMeasures();
        this.queryResults = new QueryResults();
        this.dataset = dataset;
        this.tti = tti;
        this.accumulatorCounter = 0;
        this.freqLevel = TimeSeriesIndexUtil.getTemporalLevelIndex(TimeSeriesIndexUtil.calculateFreqLevel(query.getFrom(), query.getTo())) + 1;
        CsvParserSettings parserSettings = tti.createCsvParserSettings();
        parser = new CsvParser(parserSettings);
    }

    public QueryResults prepareQueryResults(CsvTreeNode root,
                                            HashMap<Integer, Double[]> filter) throws IOException {
        List<Integer> startLabels = getLabels(query.getFrom());
        List<Integer> endLabels = getLabels(query.getTo());
        fileInputStream = new FileInputStream(tti.getCsv());
        this.processQueryNodes(root, startLabels, endLabels, true, true, 0);
        fileInputStream.close();
        queryResults.setMeasureStats(tti.getMeasureStats());
        return queryResults;
    }

    public double[] nodeSelection(CsvTreeNode treeNode) throws IOException {
        List<Double> filteredVals = new ArrayList<Double>();
        if(filter != null){
            Boolean filterCheck = filter.entrySet().stream().anyMatch(e ->
                treeNode.getStats().get(e.getKey()).getAverage() < e.getValue()[0] ||
                treeNode.getStats().get(e.getKey()).getAverage() > e.getValue()[1]);
        if(!filterCheck){
            measures.forEach(mez -> {
                filteredVals.add(treeNode.getStats().get(mez).getAverage());
            });
        };
        return filteredVals.stream().mapToDouble(val -> val).toArray();
        }else{
        return measures.stream()
        .mapToDouble(mes-> treeNode.getStats().get(mes).getAverage())
        .toArray();
        }
    }

    public double[] nodeSelectionFromFile(DoubleSummaryStatistics[] statsAccumulators) throws IOException {
        if(filter != null){
        Boolean filterCheck = filter.entrySet().stream().anyMatch(e -> {
           accumulatorCounter++;
           return (statsAccumulators[accumulatorCounter - 1].getAverage() < e.getValue()[0] ||
            statsAccumulators[accumulatorCounter - 1].getAverage() > e.getValue()[1]);
        });
        accumulatorCounter = 0;
        if(!filterCheck){
            return Arrays.stream(statsAccumulators).mapToDouble(DoubleSummaryStatistics::getAverage).toArray();
        }else{
            return new ArrayList<Double>().stream().mapToDouble(m -> m).toArray();
        }
        }else{
            return Arrays.stream(statsAccumulators).mapToDouble(DoubleSummaryStatistics::getAverage).toArray();
        }
    }

    private void setNodeData(long timestamp, double[] values)  {
        Map<Integer, List<DataPoint>> data = queryResults.getData() == null ? new HashMap<>() : queryResults.getData();
        int i = 0;
        for(int measure : measures) {
            data.computeIfAbsent(measure, m -> new ArrayList<>()).add(
                new ImmutableDataPoint(timestamp, values[i]));
            i++;
        }
        queryResults.setData(data);
    }

    public void processNode(CsvTreeNode treeNode) throws IOException {
        if (treeNode.getLevel() == freqLevel) {
            long timestamp = TimeSeriesIndexUtil.getTimestampFromLocalDateTime(getCurrentNodeDateTime());
            double[] values = nodeSelection(treeNode);
            setNodeData(timestamp, values);
        } else {
            fileInputStream.getChannel().position(treeNode.getFileOffsetStart());
            String[] row;
            LocalDateTime previousDate, currentDate = null;
            DoubleSummaryStatistics[] statsAccumulators = new DoubleSummaryStatistics[measures.size()];
            for (int i = 0; i < statsAccumulators.length; i++) {
                statsAccumulators[i] = new DoubleSummaryStatistics();
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream, StandardCharsets.UTF_8));
            TimeRange queryRange = new TimeRange(query.getFrom(), query.getTo());
            for (int i = 0; i < treeNode.getDataPointCount(); i++) {
                String s;
                while ((s = bufferedReader.readLine()).isEmpty()) ;
                row = this.parser.parseLine(s);
                queryResults.setIoCount(queryResults.getIoCount() + 1);
                previousDate = currentDate;

                currentDate = tti.parseStringToDate(row[dataset.getMeasureIndex(dataset.getTimeCol())])
                    .truncatedTo(TimeSeriesIndexUtil.TEMPORAL_HIERARCHY
                        .get(freqLevel - 1)
                        .getBaseUnit());
                if (!currentDate.equals(previousDate) && previousDate != null) {
                    long previousDateTimestamp = TimeSeriesIndexUtil.getTimestampFromLocalDateTime(previousDate);
                    if (queryRange.contains(TimeSeriesIndexUtil.getTimestampFromLocalDateTime(previousDate))) {
                        setNodeData(previousDateTimestamp, nodeSelectionFromFile(statsAccumulators));
                    }
                    statsAccumulators = new DoubleSummaryStatistics[measures.size()];
                    for (int j = 0; j < statsAccumulators.length; j++) {
                        statsAccumulators[j] = new DoubleSummaryStatistics();
                    }
                }
                for (int j = 0; j < measures.size(); j++) {
                    statsAccumulators[j].accept(Double.parseDouble(row[measures.get(j)]));
                }
                long currentDateTimestamp = TimeSeriesIndexUtil.getTimestampFromLocalDateTime(currentDate);
                if (i == treeNode.getDataPointCount() - 1 && (queryRange.contains(TimeSeriesIndexUtil.getTimestampFromLocalDateTime(currentDate)))) {
                    setNodeData(currentDateTimestamp, nodeSelectionFromFile(statsAccumulators));
                }
            }
        }
    }

    private void processQueryNodes(CsvTreeNode node, List<Integer> startLabels,
                                   List<Integer> endLabels, boolean isFirst, boolean isLast, int level) throws IOException {
        stack.push(node);
        // we are at a leaf node
        Collection<TreeNode> children = node.getChildren();
        if (node.getLevel() == freqLevel || children == null || children.isEmpty()) {
            processNode(node);
            stack.pop();
            return;
        }

        // these are the children's filters
        int start = startLabels.get(level);
        int end = endLabels.get(level);
        /* We filter in each level only in the first node and the last. If we are on the first node, we get everything that is from the start filter
         * and after. Else if we are in the last node we get everything before the end filter. Finally, if we re in intermediary nodes we get all children
         * that are below the filtered values of the current node.*/

        if (isFirst)
            children = children.stream().filter(child -> child.getLabel() >= start).collect(Collectors.toList());
        if (isLast)
            children = children.stream().filter(child -> child.getLabel() <= end).collect(Collectors.toList());
        for (TreeNode child : children) {
            // The child's first node will be the first node of the current first node and the same for the end
            boolean childIsFirst = child.getLabel() == start && isFirst;
            boolean childIsLast = child.getLabel() == end && isLast;
            processQueryNodes((CsvTreeNode) child, startLabels, endLabels, childIsFirst, childIsLast, level + 1);
        }

        stack.pop();

    }

    private LocalDateTime getCurrentNodeDateTime() {
        LocalDateTime dateTime = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
        for (int i = 1; i <= freqLevel; i++) {
            dateTime = dateTime.with(TimeSeriesIndexUtil.TEMPORAL_HIERARCHY.get(i - 1), stack.get(i).getLabel());
        }
        return dateTime;
    }

    private List<Integer> getLabels(long timestamp) {
        List<Integer> labels = new ArrayList<>();
        for (TemporalField temporalField : TimeSeriesIndexUtil.TEMPORAL_HIERARCHY) {
            labels.add(TimeSeriesIndexUtil.getLocalDateTimeFromTimestamp(timestamp).get(temporalField));
        }
        return labels;
    }

}
