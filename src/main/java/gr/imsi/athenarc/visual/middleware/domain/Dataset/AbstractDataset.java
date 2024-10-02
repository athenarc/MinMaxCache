package gr.imsi.athenarc.visual.middleware.domain.Dataset;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

import java.time.Duration;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.*;

public abstract class AbstractDataset implements Serializable {


    private static final long serialVersionUID = 1L;
    @NotNull
    private String id;
    private String[] header;
    private String schema;
    private String table;
    private TimeRange timeRange;
    private Duration samplingInterval;

    public AbstractDataset(){}

    public AbstractDataset(String id, String schema, String table){
        this.id = id;
        this.schema = schema;
        this.table = table;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public String[] getHeader() { return header; }

    public void setHeader(String[] header) { this.header = header; }

    public Duration getSamplingInterval() {
        return samplingInterval;
    }

    public void setSamplingInterval(Duration samplingInterval) {
        this.samplingInterval = samplingInterval;
    }

    public List<Integer> getMeasures(){return null;}

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
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

    @Override
    public int hashCode() {
        return 31;
    }
}
