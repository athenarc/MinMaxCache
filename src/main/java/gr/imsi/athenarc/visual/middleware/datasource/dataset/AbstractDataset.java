package gr.imsi.athenarc.visual.middleware.datasource.dataset;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import java.util.*;


public abstract class AbstractDataset {


    // yyyy-MM-dd HH:mm:ss.SSS
    private static String DEFAULT_FORMAT = "yyyy-MM-dd[ HH:mm:ss.SSS]";

    private String id;

    private String[] header;
    private String schema;
    private String tableName;
    private String timeFormat;
    private TimeRange timeRange;

    private long samplingInterval;

    public AbstractDataset(){}

    public AbstractDataset(String id, String schema, String tableName){
        this(id, schema, tableName, DEFAULT_FORMAT);
    }

    // Constructor with time format
    public AbstractDataset(String id, String schema, String tableName, String timeFormat){
        this.id = id;
        this.schema = schema;
        this.tableName = tableName;
        this.timeFormat = timeFormat;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TimeInterval getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public String[] getHeader() { return header; }

    public void setHeader(String[] header) { this.header = header; }

    public long getSamplingInterval() {
        return samplingInterval;
    }

    public void setSamplingInterval(long samplingInterval) {
        this.samplingInterval = samplingInterval;
    }

    public List<Integer> getMeasures(){return null;}

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractDataset)) {
            return false;
        }
        return id != null && id.equals(((AbstractDataset) o).id);
    }
}

