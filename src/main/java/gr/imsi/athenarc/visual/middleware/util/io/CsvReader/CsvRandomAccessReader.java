package gr.imsi.athenarc.visual.middleware.util.io.CsvReader;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CsvRandomAccessReader extends RandomAccessReader {
    private static final Logger LOG = LoggerFactory.getLogger(CsvRandomAccessReader.class);

    private final String filePath;
    private final CsvParserSettings csvParserSettings;
    private final DateTimeFormatter formatter;
    private final String timeCol;
    private final Integer timeColIndex;
    private final String delimiter;

    private final ZoneId zoneId = ZoneId.of("UTC");

    // the offset in the file of the first row (after the header, if there is one)
    private long startOffset;

    private long startTime;
    private long endTime;
    private CsvParser parser;

    private String[] parsedHeader;
    private List<Integer> measures;

    private Duration samplingInterval;

    private long meanByteSize;


    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 String timeCol, List<Integer> measures, String delimiter, Boolean hasHeader) throws IOException {
        this(filePath, formatter, timeCol, measures, delimiter, hasHeader, Charset.defaultCharset());
    }

    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 String timeCol, List<Integer> measures, String delimiter, Boolean hasHeader,
                                 Charset charset) throws IOException {
        this(filePath, formatter, timeCol, measures, delimiter, hasHeader, Charset.defaultCharset(), null, -1);
    }

    public CsvRandomAccessReader(String filePath, DateTimeFormatter formatter,
                                 String timeCol, String delimiter, Boolean hasHeader) throws IOException {
        this(filePath, formatter, timeCol, null, delimiter, hasHeader, Charset.defaultCharset(), null, -1);

    }


    public CsvRandomAccessReader(String filePath,
                                 DateTimeFormatter formatter,
                                 String timeCol, List<Integer> measures, String delimiter, Boolean hasHeader,
                                 Charset charset, Duration samplingInterval, long meanByteSize) throws IOException {
        super(new File(filePath), charset);
        this.filePath = filePath;
        this.formatter = formatter;
        this.timeCol = timeCol;
        this.delimiter = delimiter;
        this.csvParserSettings = this.createCsvParserSettings();
        this.parser = new CsvParser(csvParserSettings);

        if (hasHeader) {
            // we first parse header, before selecting specific columns for the parser to process
            parsedHeader = parseLine(this.readNewLine());
        }
        this.timeColIndex = Arrays.stream(parsedHeader).collect(Collectors.toList()).indexOf(timeCol);
        updateMeasures(measures);

        // set start offset to offset of first data point
        startOffset = current;
        startTime = parseStringToTimestamp(parser.parseLine(this.readNewLine())[timeColIndex]);

        if (meanByteSize <= 0 || samplingInterval == null) {
            computeFileStats();
        } else {
            this.meanByteSize = meanByteSize;
            this.samplingInterval = samplingInterval;
        }

        // Reset file pointer to first data point
        this.setDirection(DIRECTION.FORWARD);
        this.seek(startOffset);

        LOG.info("Created reader for file {} with size {} bytes.", filePath, this.length());
        LOG.info("Start timestamp for file {} is {} at offset {}.", filePath, DateTimeUtil.format(startTime), startOffset);

    }


    private void computeFileStats() throws IOException {
        long secondTime = parseStringToTimestamp(parser.parseLine(this.readNewLine())[timeColIndex]);
        samplingInterval = Duration.of(secondTime - startTime, ChronoUnit.MILLIS);
        LOG.info("Sampling interval for file {}: {}.", filePath, samplingInterval);

        goToEnd();

        String lastLine = this.readNewLine();
        this.endTime = parseStringToTimestamp(parser.parseLine(lastLine)[timeColIndex]);
        LOG.info("End time for file {}: {}", filePath, endTime);

        long estimatedSamples = Duration.of(endTime - startTime, ChronoUnit.MILLIS).dividedBy(samplingInterval);
        LOG.info("Number of estimated rows in file {}: {}", filePath, estimatedSamples);
        meanByteSize = (this.length() - startOffset) / estimatedSamples;
        LOG.info("Mean row byte size in file {}: {}", filePath, meanByteSize);

    }

    private long findPosition(long time) {
        return Duration.of(time - startTime, ChronoUnit.MILLIS).dividedBy(samplingInterval);
    }

    private long findProbabilisticOffset(long position) throws IOException {
        return Math.min(startOffset + meanByteSize * position, this.channel.size());
    }

    private long findOffset(long time) throws IOException {
        long position = findPosition(time);
        long probabilisticOffset = findProbabilisticOffset(position);

        this.setDirection(DIRECTION.FORWARD);
        this.seek(probabilisticOffset);

        // read a new line in case the probabilisticOffset does not match the start of a new line
        // todo: find a better way to handle this. Perhaps read the previous character to check if eol
        this.readNewLine();
        long prevOffset = this.getFilePointer();

        String line = this.readNewLine();

        long firstTimeFound = parseStringToTimestamp(this.parseLine(line)[timeColIndex]);

        long timeFound;
        if (firstTimeFound > time) {
            this.setDirection(DIRECTION.BACKWARD);
            while (true) {
                line = this.readNewLine();
                prevOffset = this.getFilePointer();
                timeFound = parseStringToTimestamp(this.parseLine(line)[timeColIndex]);
                if (timeFound <= time) return prevOffset;
            }
        } else if (firstTimeFound == time) {
            return prevOffset - 1;
        } else {
            while (true) {
                this.setDirection(DIRECTION.FORWARD);
                prevOffset = this.getFilePointer();
                line = this.readNewLine();
                timeFound = parseStringToTimestamp(this.parseLine(line)[timeColIndex]);
                if (timeFound >= time) return prevOffset - 1;
            }
        }
    }

    public void seekTimestamp(long timestamp) throws IOException {
        long offset = findOffset(timestamp);
        seek(offset);
    }

    public void goToEnd() throws IOException {
        this.seek(this.length() - 1);
        this.setDirection(DIRECTION.BACKWARD);
    }


    public String[] parseNext() throws IOException {
        String line;
        while ((line = readNewLine()) != null && line.isBlank());
        if (line == null) return null;
        return this.parser.parseLine(line);
    }

    private String[] parseLine(String line) {
        return this.parser.parseLine(line);
    }


    public String[] getParsedHeader() {
        return parsedHeader;
    }

    public Duration getSamplingInterval() {
        return samplingInterval;
    }

    public long getMeanByteSize() {
        return meanByteSize;
    }

    private void updateMeasures(List<Integer> measures) {
        if (measures != null){
            this.measures = measures;
            List<Integer> tmpList = new ArrayList<>();
            tmpList.add(this.timeColIndex);
            tmpList.addAll(this.measures);
            this.csvParserSettings.selectIndexes(tmpList.toArray(new Integer[0]));
            this.parser = new CsvParser(this.csvParserSettings);
        }
    }

    private long parseStringToTimestamp(String s) {
        ZonedDateTime zonedDateTime = LocalDateTime.parse(s, formatter).atZone(zoneId);
        return zonedDateTime.toInstant().toEpochMilli();
    }

    private CsvParserSettings createCsvParserSettings() {
        CsvParserSettings parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(this.delimiter.charAt(0));
        parserSettings.setIgnoreLeadingWhitespaces(false);
        parserSettings.setIgnoreTrailingWhitespaces(false);
        parserSettings.setLineSeparatorDetectionEnabled(true);
        parserSettings.setHeaderExtractionEnabled(false);
        // needed in order to be able to access measure values by col index
        parserSettings.setColumnReorderingEnabled(false);
        return parserSettings;
    }

}
