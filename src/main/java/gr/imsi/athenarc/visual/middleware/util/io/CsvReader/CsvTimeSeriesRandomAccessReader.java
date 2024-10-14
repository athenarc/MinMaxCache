package gr.imsi.athenarc.visual.middleware.util.io.CsvReader;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CsvTimeSeriesRandomAccessReader extends RandomAccessReader {
    private static final Logger LOG = LoggerFactory.getLogger(CsvTimeSeriesRandomAccessReader.class);

    private final CsvParserSettings csvParserSettings;
    private List<Integer> measures;
    private final String timeColumn;
    private final Integer timeColumnIndex;
    private final String delimiter;
    private final DateTimeFormatter formatter;

    // the offset in the file of the first row (after the header, if there is one)
    private long startOffset;

    private long startTime;
    private long endTime;
    private CsvParser parser;

    private String[] parsedHeader;

    private long samplingInterval;

    private long meanByteSize;

    public CsvTimeSeriesRandomAccessReader(String filePath, String timeColumn, 
        List<Integer> measures, String delimiter, Boolean hasHeader, DateTimeFormatter formatter) throws IOException {
        super(new File(filePath));
        if (delimiter == null || delimiter.length() != 1) {
            throw new IllegalArgumentException("Delimiter must be a single character.");
        }
        if (length() == 0) {
            throw new IOException("The file is empty.");
        }
        this.measures = measures;
        this.delimiter = delimiter;
        this.csvParserSettings = createCsvParserSettings();
        this.formatter = formatter;
        this.timeColumn = timeColumn;
        this.parser = new CsvParser(csvParserSettings);
        if (hasHeader) {
            // we first parse header, before selecting specific columns for the parser to process
            parsedHeader = this.parseLine(readNewLine());
        }
       
        this.timeColumnIndex = Arrays.stream(parsedHeader).collect(Collectors.toList()).indexOf(timeColumn);
        if (timeColumnIndex == -1) {
            throw new IllegalArgumentException("Time column '" + timeColumn + "' not found in the header.");
        }
        // Update parser measures now that we have the header
        updateMeasures();

        // set start offset to offset of first data point
        this.startOffset = current;

        // Read the first data line to initialize startTime
        String firstDataLine = readNewLine();
        if (firstDataLine == null) {
            throw new IOException("No data available after the header in the file.");
        }
        this.startTime = parseTimestampFromLine(this.parseLine(firstDataLine)[timeColumnIndex]);

        LOG.info("Start timestamp for file {} is {} at offset {}.", filePath, DateTimeUtil.format(startTime), startOffset);
        computeFileStats();

        // Reset file pointer to first data point
        this.setDirection(DIRECTION.FORWARD);
        this.seek(startOffset);
        LOG.info("Created reader for file {} with size {} bytes.", filePath, length());

    }

    private void updateMeasures() {
        if(measures != null && !measures.isEmpty() ){
            List<Integer> tmpList = new ArrayList<>();
            tmpList.add(timeColumnIndex);
            tmpList.addAll(measures);
            this.csvParserSettings.selectIndexes(tmpList.toArray(new Integer[0]));
            this.parser = new CsvParser(this.csvParserSettings);
        }
    }

    private CsvParserSettings createCsvParserSettings() {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(this.delimiter.charAt(0));
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        parserSettings.setLineSeparatorDetectionEnabled(true);
        parserSettings.setHeaderExtractionEnabled(false);
        // needed in order to be able to access measure values by col index
        parserSettings.setColumnReorderingEnabled(true);
        return parserSettings;
    }

    private void computeFileStats() throws IOException {
        String nextLine = readNewLine();
        if (nextLine == null) {
            throw new IOException("Insufficient data to compute file statistics.");
        }
        long secondTime = parseTimestampFromLine(parser.parseLine(nextLine)[timeColumnIndex]);
        if (secondTime == startTime) {
            throw new IOException("Sampling interval cannot be zero.");
        }
        this.samplingInterval = secondTime - startTime;
        LOG.info("Sampling interval for file {}: {} ms.", filePath, samplingInterval);

        goToEnd();

        String lastLine = readNewLine();
        this.endTime = parseTimestampFromLine(parser.parseLine(lastLine)[timeColumnIndex]);
        LOG.info("End timestamp for file {}: {}", filePath, DateTimeUtil.format(endTime));

        long estimatedSamples = DateTimeUtil.numberOfIntervals(startTime, endTime, samplingInterval);
        LOG.info("Number of estimated rows in file {}: {}", filePath, estimatedSamples);
        this.meanByteSize = (length() - startOffset) / estimatedSamples;
        LOG.info("Mean row byte size in file {}: {}", filePath, meanByteSize);

    }

    private long findPosition(long time) {
        return DateTimeUtil.indexInInterval(startTime, endTime, samplingInterval, time);
    }

    private long findProbabilisticOffset(long position) throws IOException {
        return Math.min(startOffset + meanByteSize * position, channel.size());
    }

    private long findOffset(long time) throws IOException {
        long position = findPosition(time);
        long probabilisticOffset = findProbabilisticOffset(position);

        this.setDirection(DIRECTION.FORWARD);
        this.seek(probabilisticOffset);

        // read a new line in case the probabilisticOffset does not match the start of a new line
        // todo: find a better way to handle this. Perhaps read the previous character to check if eol
        readNewLine();
        long prevOffset = getFilePointer();

        String line = readNewLine();

        long firstTimeFound = parseTimestampFromLine(this.parseLine(line)[timeColumnIndex]);

        long timeFound;

        // Define maximum iterations to prevent infinite loops
        int maxIterations = 1000000; // Adjust as appropriate
        int iterations = 0;
        if (firstTimeFound > time) {
            this.setDirection(DIRECTION.BACKWARD);
            while (iterations ++ < maxIterations) {
                if (getFilePointer() <= startOffset) {
                    throw new IOException("Timestamp not found before start of data.");
                }
                line = readNewLine();
                prevOffset = getFilePointer();
                timeFound = parseTimestampFromLine(this.parseLine(line)[timeColumnIndex]);
                if (timeFound <= time) return prevOffset;
            }
        } else if (firstTimeFound == time) {
            return prevOffset - 1;
        } else {
            while (iterations ++ < maxIterations) {
                if (isEOF()) {
                    throw new IOException("Timestamp not found before end of file.");
                }
                this.setDirection(DIRECTION.FORWARD);
                prevOffset = getFilePointer();
                line = readNewLine();
                timeFound = parseTimestampFromLine(this.parseLine(line)[timeColumnIndex]);
                if (timeFound >= time) return prevOffset - 1;
            }
        }
        throw new IOException("Maximum number of iterations reached, timestamp not found.");
    }

    public void seekTimestamp(long timestamp) throws IOException {
        long offset = findOffset(timestamp);
        this.seek(offset);
    }

    public void goToEnd() throws IOException {
        this.seek(length() - 1);
        this.setDirection(DIRECTION.BACKWARD);
    }


    public String[] parseNext() throws IOException {
        String line;
        while ((line = readNewLine()) != null && line.isBlank());
        if (line == null) return null;
        return parser.parseLine(line);
    }

    private String[] parseLine(String line) {  
        return parser.parseLine(line);
    }

    public String[] getParsedHeader() {
        return parsedHeader;
    }

    public long getSamplingInterval() {
        return samplingInterval;
    }

    public long getMeanByteSize() {
        return meanByteSize;
    }

    private long parseTimestampFromLine(String line) throws IOException {
        try {
            String[] fields = this.parseLine(line);
            return DateTimeUtil.parseDateTimeString(fields[timeColumnIndex], formatter);
        } catch (Exception e) {
            LOG.error("Failed to parse timestamp from line: {}", line, e);
            throw new IOException("Failed to parse timestamp.", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (parser != null) {
            parser.stopParsing();
        }
        super.close();
    }

}
