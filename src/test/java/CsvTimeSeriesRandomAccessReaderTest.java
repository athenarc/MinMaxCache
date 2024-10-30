import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
import gr.imsi.athenarc.visual.middleware.util.io.CsvReader.CsvTimeSeriesRandomAccessReader;
import gr.imsi.athenarc.visual.middleware.util.io.CsvReader.DIRECTION;

public class CsvTimeSeriesRandomAccessReaderTest {

    private static final Logger LOG = LoggerFactory.getLogger(CsvTimeSeriesRandomAccessReaderTest.class);

    private CsvTimeSeriesRandomAccessReader reader;
    private String filePath;
    private DateTimeFormatter formatter;
    private String timeColumn;
    private String delimiter;
    private Boolean hasHeader;

    @Before
    public void setUp() throws IOException {
        // Load the test CSV file from the resources folder
        URL resource = getClass().getClassLoader().getResource("test.csv");
        assertNotNull("CSV file should exist in resources", resource);
        File file = new File(resource.getFile());
        filePath = file.getPath();

        // Specify the time column, measures, and delimiter
        timeColumn = "datetime";
        delimiter = ",";
        hasHeader = true;

        // Define the date time format (adjust this to the format of your time column in the CSV file)
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // Create the CsvTimeSeriesRandomAccessReader instance
        reader = new CsvTimeSeriesRandomAccessReader(filePath, timeColumn, delimiter, hasHeader, formatter);
    }

    @After
    public void tearDown() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    @Test
    public void testInitialization() throws IOException {
        assertNotNull("Reader should be initialized", reader);
    }

    @Test
    public void testReadingHeader() {
        String[] header = reader.getParsedHeader();
        assertNotNull("Header should not be null", header);
        assertTrue("Header should contain the time column", Arrays.asList(header).contains(timeColumn));
    }

    @Test
    public void testParsingFirstRow() throws IOException {
        String[] firstRow = reader.parseNext();
        assertNotNull("First row should not be null", firstRow);
        assertEquals("First timestamp should match", "2004-02-28 00:58:46", firstRow[0]);
    }

    @Test
    public void testSeekingToFirstTimestamp() throws IOException {
        long testTimestamp = DateTimeUtil.parseDateTimeString("2004-02-28 00:58:46", formatter);
        reader.seekTimestamp(testTimestamp);

        String[] row = reader.parseNext();
        assertNotNull("Row after seeking should not be null", row);
        assertEquals("Timestamp should match", "2004-02-28 00:58:46", row[0]);
    }

    @Test
    public void testBackwardsSearch() throws IOException {
        long testTimestamp = DateTimeUtil.parseDateTimeString("2004-04-02 18:17:46", formatter);
        reader.seekTimestamp(testTimestamp);
        reader.setDirection(DIRECTION.BACKWARD);
        reader.readNewLine();
        String[] row = reader.parseNext();
        assertNotNull("Row after seeking should not be null", row);
        assertEquals("Timestamp should match", "2004-04-02 18:16:46", row[0]);
    }


    @Test
    public void testSeekingToLastTimestamp() throws IOException {
        long testTimestamp = DateTimeUtil.parseDateTimeString("2004-04-02 18:17:46", formatter);
        reader.seekTimestamp(testTimestamp);

        String[] row = reader.parseNext();
        assertNotNull("Row after seeking should not be null", row);
        assertEquals("Timestamp should match", "2004-04-02 18:17:46", row[0]);
    }

    @Test
    public void testSeekingToExistingTimestamp() throws IOException {
        long testTimestamp = DateTimeUtil.parseDateTimeString("2004-03-02 18:39:46", formatter);
        reader.seekTimestamp(testTimestamp);

        String[] row = reader.parseNext();
        assertNotNull("Row after seeking should not be null", row);
        assertEquals("Timestamp should match", "2004-03-02 18:39:46", row[0]);
    }

    @Test
    public void testSeekingToNonExistingTimestamp() throws IOException {
        long testTimestamp = DateTimeUtil.parseDateTimeString("2004-03-02 18:39:40", formatter);
        reader.seekTimestamp(testTimestamp);

        String[] row = reader.parseNext();
        assertNotNull("Row after seeking should not be null", row);
        long rowTimestamp = DateTimeUtil.parseDateTimeString(row[0], formatter);

        // Since the exact timestamp doesn't exist, we expect the next closest
        assertTrue("Row timestamp should be greater than or equal to the test timestamp", rowTimestamp >= testTimestamp);
    }

    @Test
    public void testReadingAfterEOF() throws IOException {
        reader.goToEnd();
        reader.setDirection(DIRECTION.FORWARD);

        String[] row = reader.parseNext();
        assertNull("No rows should be available after EOF", row);
    }

    @Test(expected = IOException.class)
    public void testEmptyFile() throws IOException {
        // Use an empty CSV file
        URL resource = getClass().getClassLoader().getResource("empty.csv");
        assertNotNull("Empty CSV file should exist in resources", resource);
        File file = new File(resource.getFile());
        CsvTimeSeriesRandomAccessReader emptyReader = new CsvTimeSeriesRandomAccessReader(
                file.getPath(), timeColumn, delimiter, hasHeader, formatter);

        // Attempt to parse next should fail due to insufficient data
        emptyReader.parseNext();
    }

    @Test(expected = IOException.class)
    public void testFileWithOnlyHeader() throws IOException {
        // Use a CSV file that contains only a header
        URL resource = getClass().getClassLoader().getResource("header_only.csv");
        assertNotNull("Header-only CSV file should exist in resources", resource);
        File file = new File(resource.getFile());
        CsvTimeSeriesRandomAccessReader headerOnlyReader = new CsvTimeSeriesRandomAccessReader(
                file.getPath(), timeColumn, delimiter, hasHeader, formatter);

        // Attempt to parse next should fail due to insufficient data
        headerOnlyReader.parseNext();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTimeColumn() throws IOException {
        // Use an invalid time column name
        String invalidTimeColumn = "invalid_timestamp";
        new CsvTimeSeriesRandomAccessReader(filePath, invalidTimeColumn, delimiter, hasHeader, formatter);
    }

    // @Test
    // public void testVariableSamplingInterval() throws IOException {
    //     // Use a CSV file with variable sampling intervals
    //     URL resource = getClass().getClassLoader().getResource("variable_interval.csv");
    //     assertNotNull("Variable interval CSV file should exist in resources", resource);
    //     File file = new File(resource.getFile());

    //     CsvTimeSeriesRandomAccessReader variableIntervalReader = new CsvTimeSeriesRandomAccessReader(
    //             file.getPath(), timeColumn, measures, delimiter, hasHeader, formatter);

    //     // Since sampling interval is variable, mean byte size calculation might be off
    //     assertTrue("Sampling interval should be greater than zero", variableIntervalReader.getSamplingInterval() > 0);

    //     // Attempt to seek to a timestamp
    //     long testTimestamp = DateTimeUtil.parseDateTimeString("2023-01-01 00:03:30", formatter);
    //     variableIntervalReader.seekTimestamp(testTimestamp);

    //     String[] row = variableIntervalReader.parseNext();
    //     assertNotNull("Row after seeking should not be null", row);
    //     long rowTimestamp = DateTimeUtil.parseDateTimeString(row[0], formatter);
    //     assertTrue("Row timestamp should be greater than or equal to the test timestamp", rowTimestamp >= testTimestamp);
    // }

    @Test
    public void testDifferentDelimiters() throws IOException {
        // Use a CSV file with a semicolon delimiter
        URL resource = getClass().getClassLoader().getResource("test_sem.csv");
        assertNotNull("Semicolon-delimited CSV file should exist in resources", resource);
        File file = new File(resource.getFile());

        String semicolonDelimiter = ";";
        CsvTimeSeriesRandomAccessReader semicolonReader = new CsvTimeSeriesRandomAccessReader(
                file.getPath(), timeColumn, semicolonDelimiter, hasHeader, formatter);

        // Parse the first row
        String[] firstRow = semicolonReader.parseNext();
        assertNotNull("First row should not be null", firstRow);
        assertEquals("First timestamp should match", "2004-02-28 00:58:46", firstRow[0]);

        semicolonReader.close();
    }

    @Test
    public void testParsingErrors() throws IOException {
        // Use a CSV file with a malformed row
        URL resource = getClass().getClassLoader().getResource("malformed.csv");
        assertNotNull("Malformed CSV file should exist in resources", resource);
        File file = new File(resource.getFile());

        CsvTimeSeriesRandomAccessReader malformedReader = new CsvTimeSeriesRandomAccessReader(
                file.getPath(), timeColumn, delimiter, hasHeader, formatter);

        // Read all rows and handle parsing errors
        String[] row;
        while ((row = malformedReader.parseNext()) != null) {
            // Process row or handle errors
            assertNotNull("Row should not be null despite parsing errors", row);
        }
    }

    @Test
    public void testCloseMethod() throws IOException {
        reader.close();
        try {
            reader.parseNext();
            fail("Expected IOException after closing reader");
        } catch (IOException e) {
            // Expected exception
        }
    }
}
