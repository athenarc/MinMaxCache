package gr.imsi.athenarc.visual.middleware.index.csv;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import gr.imsi.athenarc.visual.middleware.domain.*;

import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.index.TimeSeriesIndexUtil;
import gr.imsi.athenarc.visual.middleware.index.TreeNode;

import gr.imsi.athenarc.visual.middleware.domain.Dataset.CsvDataset;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalUnit;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CsvTTI {

    private static final Logger LOG = LoggerFactory.getLogger(CsvTTI.class);
    protected CsvTreeNode root;
    private Map<Integer, DoubleSummaryStatistics> measureStats;
    private CsvDataset dataset;
    private String csv;
    private int objectsIndexed = 0;
    private boolean isInitialized = false;
    private DateTimeFormatter formatter;

    public CsvTTI(String csv, CsvDataset dataset) {
        this.dataset = dataset;
        this.csv = csv;
        this.formatter =
            new DateTimeFormatterBuilder().appendPattern("")
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .toFormatter();
    }

    private TreeNode addPoint(Stack<Integer> labels, long fileOffset, String[] row) {
        if (root == null) {
            root = new CsvTreeNode(0, 0);
        }
        // adjust root node meta
        if (root.getDataPointCount() == 0) {
            root.setFileOffsetStart(fileOffset);
        }
        root.setDataPointCount(root.getDataPointCount() + 1);
        root.adjustStats(row, dataset);
        CsvTreeNode node = root;
        if (node.getDataPointCount() == 0) {
            node.setFileOffsetStart(fileOffset);
        }

        node.setDataPointCount(node.getDataPointCount() + 1);
        node.adjustStats(row, dataset);

        for (Integer label : labels) {
            CsvTreeNode child = (CsvTreeNode) node.getOrAddChild(label);
            node = child;
            if (node.getDataPointCount() == 0) {
                node.setFileOffsetStart(fileOffset);
            }
            node.setDataPointCount(node.getDataPointCount() + 1);
            node.adjustStats(row, dataset);
        }
        return node;
    }

    public void initialize(Query q0) throws IOException {
        // truncate q0 time range to query frequency level
        LocalDateTime from = TimeSeriesIndexUtil.getLocalDateTimeFromTimestamp(q0.getFrom());
        LocalDateTime to = TimeSeriesIndexUtil.getLocalDateTimeFromTimestamp(q0.getTo());
        int q0Frequency = TimeSeriesIndexUtil.getTemporalLevelIndex(TimeSeriesIndexUtil.calculateFreqLevel(q0.getFrom(), q0.getTo()));
        TemporalUnit temporalUnit = TimeSeriesIndexUtil.TEMPORAL_HIERARCHY.get(q0Frequency - 1).getBaseUnit();
        TimeRange timeRange = new TimeRange(TimeSeriesIndexUtil.getTimestampFromLocalDateTime(from.truncatedTo(temporalUnit)),
                TimeSeriesIndexUtil.getTimestampFromLocalDateTime(to.truncatedTo(temporalUnit)));

        measureStats = new HashMap<>();
        for (Integer measureIndex : dataset.getMeasures()) {
            measureStats.put(measureIndex, new DoubleSummaryStatistics());
        }

        CsvParserSettings parserSettings = createCsvParserSettings();
        // to be able to get file offset of first measurement
        parserSettings.setHeaderExtractionEnabled(false);
        CsvParser parser = new CsvParser(parserSettings);
        objectsIndexed = 0;

        parser.beginParsing(new File(csv), StandardCharsets.US_ASCII);
        long rowOffset = 0l;
        if (dataset.getHasHeader()) {
            parser.parseNext();  //skip header row
            rowOffset = parser.getContext().currentChar() - 1;
        }

        String[] row;

        int queryFrequencyLevel = q0Frequency + 1;
        while ((row = parser.parseNext()) != null) {
            LocalDateTime dateTime = parseStringToDate(row[dataset.getMeasureIndex(dataset.getTimeCol())]);

            Stack<Integer> labels = new Stack<>();
            int lastIndex = timeRange.contains(TimeSeriesIndexUtil.getTimestampFromLocalDateTime(dateTime.truncatedTo(temporalUnit))) ?
                    queryFrequencyLevel : queryFrequencyLevel - 1;
            for (int i = 0; i < lastIndex; i++) {
                labels.add(dateTime.get(TimeSeriesIndexUtil.TEMPORAL_HIERARCHY.get(i)));
            }
            for (Integer measureIndex : dataset.getMeasures()) {
                measureStats.get(measureIndex).accept(Double.parseDouble(row[measureIndex]));
            }

            this.addPoint(labels, rowOffset, row);
            objectsIndexed++;
            rowOffset = parser.getContext().currentChar() - 1;
        }
        parser.stopParsing();
        isInitialized = true;

       /* measureStats = statsMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            e -> new MeasureStats(e.getValue().getAverage(), e.getValue().getMin(), e.getValue().getMax())));*/

        LOG.debug("Indexing Complete. Total Indexed Objects: " + objectsIndexed);
//        traverse(root);
    }

    public synchronized QueryResults executeQuery(Query query) throws IOException {
        if (!isInitialized) {
            initialize(query);
        }
        CsvQueryProcessor queryProcessor = new CsvQueryProcessor(query, dataset, this);
        return queryProcessor.prepareQueryResults(root, query.getFilter());
    }

    public void traverse(TreeNode node) {
        LOG.debug(node.toString());
        Collection<TreeNode> children = node.getChildren();
        if (children != null && !children.isEmpty()) {
            for (TreeNode child : children) {
                traverse(child);
            }
        }
    }

    public LocalDateTime parseStringToDate(String s) {
        return LocalDateTime.parse(s, formatter);
    }

    public CsvParserSettings createCsvParserSettings() {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(((CsvDataset) dataset).getDelimiter().charAt(0));
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        parserSettings.setLineSeparatorDetectionEnabled(true);

        return parserSettings;
    }

    public String getCsv() {
        return csv;
    }

    public Map<Integer, DoubleSummaryStatistics> getMeasureStats() {
        return measureStats;
    }

}
