package gr.imsi.athenarc.visual.middleware.domain.Dataset;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.Table;

import java.time.Duration;

import javax.validation.constraints.NotNull;

import com.influxdb.annotations.Column;

import java.io.Serializable;
import java.util.*;


@Entity
@Table(name = "datasets") // Root table for the hierarchy
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class AbstractDataset implements Serializable {


    private static final long serialVersionUID = 1L;

    @NotNull
    @Id
    private String id;

    private String[] header;
    private String schema;
    private String tableName;
    private TimeRange timeRange;

    @Column(name = "sampling_interval")
    private Duration samplingInterval;

    public AbstractDataset(){}

    public AbstractDataset(String id, String schema, String tableName){
        this.id = id;
        this.schema = schema;
        this.tableName = tableName;
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
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
