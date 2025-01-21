package gr.imsi.athenarc.visual.middleware.domain.csv;

public class CsvConfiguration {

    private final String path, timeFormat, timeCol, delimiter;
    private final boolean hasHeader;

    public CsvConfiguration(String path, String timeFormat, String timeCol, String delimiter, boolean hasHeader) {
        this.path = path;
        this.timeFormat = timeFormat;
        this.timeCol = timeCol;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
    }

    public String getPath() {
        return path;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public String getTimeCol() {
        return timeCol;
    }

    public boolean getHasHeader() {
        return hasHeader;
    }

    public String getDelimiter() {
        return delimiter;
    }
}
