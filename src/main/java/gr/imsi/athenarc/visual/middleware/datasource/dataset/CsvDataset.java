package gr.imsi.athenarc.visual.middleware.datasource.dataset;

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.datasource.csv.CsvTimeSeriesRandomAccessReader;
import gr.imsi.athenarc.visual.middleware.domain.IntervalTree;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

public class CsvDataset extends AbstractDataset {

    private static final Logger LOG = LoggerFactory.getLogger(CsvDataset.class);

    private String timeCol;
    private String filePath;
    private String timeFormat;
    private String delimiter;
    private boolean hasHeader;
    
    private IntervalTree<TimeSeriesCsv> fileTimeRangeTree = new IntervalTree<TimeSeriesCsv>();
    public CsvDataset(){}

    // Abstract class implementation
    public CsvDataset(String id, String schema, String table, String timeFormat){
        super(id, schema, table, timeFormat);
    }

    public CsvDataset(String filePath, String timeFormat, String timeCol, String delimiter, boolean hasHeader) throws IOException {
        super(filePath, filePath, filePath, timeFormat);
        this.filePath = filePath;
        this.timeCol = timeCol;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
        this.timeFormat = timeFormat;

        File datasetFile = new File(filePath);
        if (datasetFile.isDirectory()) {
            // Handle multiple files in the folder
            processFolder(datasetFile);
        } else {
            // Handle single file as before
            processSingleFile(datasetFile);
        }
    }

    private void processFolder(File folder) throws IOException {
        long overallStart = Long.MAX_VALUE;
        long overallEnd = Long.MIN_VALUE;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
    
        String[] expectedHeader = null;
        long expectedSamplingInterval = -1;
    
        for (File file : folder.listFiles()) {
            if (file.getName().endsWith(".csv")) { // Ensuring we only process CSV files
                CsvTimeSeriesRandomAccessReader reader = new CsvTimeSeriesRandomAccessReader(
                    file.getPath(), timeCol, delimiter, hasHeader, formatter
                );
                TimeInterval fileTimeRange = reader.getTimeRange();
                String[] fileHeader = reader.getParsedHeader();
                long fileSamplingInterval = reader.getSamplingInterval();
    
                // Validate header and sampling interval consistency
                if (expectedHeader == null && expectedSamplingInterval == -1) {
                    expectedHeader = fileHeader;
                    expectedSamplingInterval = fileSamplingInterval;
                    setHeader(expectedHeader);
                    setSamplingInterval(expectedSamplingInterval);
                } else {
                    if (!Arrays.equals(expectedHeader, fileHeader)) {
                        reader.close();
                        throw new IOException("Inconsistent header detected in file: " + file.getPath());
                    }
                    if (expectedSamplingInterval != fileSamplingInterval) {
                        reader.close();
                        throw new IOException("Inconsistent sampling interval detected in file: " + file.getPath());
                    }
                }
    
                // Update the file time range mapping and overall dataset range
                fileTimeRangeTree.insert(new TimeSeriesCsv(fileTimeRange, file.getPath()));
                overallStart = Math.min(overallStart, fileTimeRange.getFrom());
                overallEnd = Math.max(overallEnd, fileTimeRange.getTo());
    
                reader.close();
            }
        }
    
        // Set the dataset's overall time range to encompass all files
        setTimeRange(new TimeRange(overallStart, overallEnd));
    }    

    private void processSingleFile(File file) throws IOException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeFormat);
        CsvTimeSeriesRandomAccessReader reader = new CsvTimeSeriesRandomAccessReader(
            file.getPath(), timeCol, delimiter, hasHeader, formatter
        );

        setTimeRange(reader.getTimeRange());
        setHeader(reader.getParsedHeader());
        setSamplingInterval(reader.getSamplingInterval());
        fileTimeRangeTree.insert(new TimeSeriesCsv(getTimeRange(), file.getPath()));
        reader.close();
    }
    

    @Override
    public List<Integer> getMeasures() {
        int[] measures = new int[getHeader().length];
        for(int i = 0; i < measures.length; i++)
            measures[i] = i;
        return Arrays.stream(measures)
                .boxed()
                .collect(Collectors.toList());
    }

    public String getTimeCol() {
        return timeCol;
    }

    public void setTimeCol(String timeCol) {
        this.timeCol = timeCol;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public boolean getHasHeader() {
        return hasHeader;
    }

    public int getTimeColumnIndex(){
        return Arrays.asList(getHeader()).indexOf(getTimeCol());
    }

    public IntervalTree<TimeSeriesCsv> getFileTimeRangeTree() {
        return fileTimeRangeTree;
    }

    /* Returns the interval tree of file names in a map form */
    public Map<String, TimeInterval> getFileTimeRangeTreeMap() {
        Iterator<TimeSeriesCsv> fileTimeRangeTreeIterator = fileTimeRangeTree.iterator();
        Map<String, TimeInterval> fileTimeRangeMap = new java.util.HashMap<>();
        while(fileTimeRangeTreeIterator.hasNext()){
            TimeSeriesCsv timeSeriesCsv = fileTimeRangeTreeIterator.next();
            fileTimeRangeMap.put(timeSeriesCsv.getFilePath(), 
                new TimeRange(timeSeriesCsv.getTimeRange().getFrom(), timeSeriesCsv.getTimeRange().getTo()));
        }
        return fileTimeRangeMap;
    }


}
