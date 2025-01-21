package gr.imsi.athenarc.visual.middleware.domain.dataset;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.Table;

import javax.validation.constraints.NotNull;

import com.influxdb.annotations.Column;

import java.io.Serializable;
import java.util.*;


@Entity
@Table(name = "datasets") // Root table for the hierarchy
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class AbstractDataset implements Serializable {


    private static final long serialVersionUID = 1L;
    // yyyy-MM-dd HH:mm:ss.SSS
    private static String DEFAULT_FORMAT = "yyyy-MM-dd[ HH:mm:ss.SSS]";
    @NotNull
    @Id
    private String id;

    private String[] header;
    private String schema;
    private String tableName;
    private String timeFormat;
    private TimeRange timeRange;

    @Column(name = "sampling_interval")
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

    @Override
    public int hashCode() {
        return 31;
    }
}
