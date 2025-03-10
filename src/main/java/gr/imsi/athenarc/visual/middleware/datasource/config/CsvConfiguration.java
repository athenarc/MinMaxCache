package gr.imsi.athenarc.visual.middleware.datasource.config;

public class CsvConfiguration implements DataSourceConfiguration {

    private final String path, timeFormat, id, timeCol, delimiter;
    private final boolean hasHeader;

    public CsvConfiguration(String path, String id, String timeFormat, String timeCol, String delimiter, boolean hasHeader) {
        this.path = path;
        this.id = id;
        this.timeFormat = timeFormat;
        this.timeCol = timeCol;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
    }

    public String getId() {
        return id;
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
