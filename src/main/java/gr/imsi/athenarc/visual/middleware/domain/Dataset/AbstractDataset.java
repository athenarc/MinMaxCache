package gr.imsi.athenarc.visual.middleware.domain.Dataset;

import gr.imsi.athenarc.visual.middleware.domain.DataFileInfo;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

import java.time.Duration;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.*;

public abstract class AbstractDataset implements Serializable {


    private static final long serialVersionUID = 1L;
    private Duration samplingInterval;
    @NotNull
    private String path;
    @NotNull
    private String id;
    private Integer resType; // 0: panel, 1: turbine
    private String[] header;
    private String timeCol;
    private String idCol;
    private String valueCol;
    private String schema;
    private String table;
    private String timeFormat;

    private TimeRange timeRange;
    private String type;
    List<DataFileInfo> fileInfoList = new ArrayList<>();

    public AbstractDataset(){}

    public AbstractDataset(String id){
        this.id = id;
    }

    public AbstractDataset(String path, String id, String table, String timeCol, String timeFormat) {
        this.path = path;
        this.id = id;
        this.table = table;
        this.timeCol = timeCol;
        this.timeFormat = timeFormat;
    }

    public AbstractDataset(String id, String table, String schema, String timeFormat, String timeCol, String idCol, String valueCol) {
        this.id = id;
        this.timeCol = timeCol;
        this.idCol = idCol;
        this.valueCol = valueCol;
        this.timeFormat = timeFormat;
        this.schema = schema;
        this.table = table;
    }

    public AbstractDataset(String id, String table, String schema, String timeFormat) {
        this.id = id;
        this.table = table;
        this.schema = schema;
        this.timeFormat = timeFormat;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getResType() {
        return resType;
    }

    public void setResType(Integer resType) {
        this.resType = resType;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
        this.timeRange = timeRange;
    }

    public String[] getHeader() { return header; }

    public void setHeader(String[] header) { this.header = header; }

    public String getType() { return type; }

    public void setType(String type) { this.type = type; }


    public String getTimeCol() {
        return timeCol;
    }

    public void setTimeCol(String timeCol) {
        this.timeCol = timeCol;
    }

    public Duration getSamplingInterval() {
        return samplingInterval;
    }

    public void setSamplingInterval(Duration samplingInterval) {
        this.samplingInterval = samplingInterval;
    }

    public List<DataFileInfo> getFileInfoList() {
        return fileInfoList;
    }

    public void setFileInfoList(List<DataFileInfo> fileInfoList) {
        this.fileInfoList = fileInfoList;
    }

    public List<Integer> getMeasures(){return null;}

    public String getSchema() {
        return schema;
    }

    public void setIdCol(String idCol) {
        this.idCol = idCol;
    }

    public void setValueCol(String valueCol) {
        this.valueCol = valueCol;
    }

    public String getIdCol() {
        return idCol;
    }

    public String getValueCol() {
        return valueCol;
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


    @Override
    public String toString() {
        return "AbstractDataset{" +
                ", timeCol='" + timeCol + '\'' +
                ", idCol='" + idCol + '\'' +
                ", valueCol='" + valueCol + '\'' +
                "samplingInterval=" + samplingInterval +
                ", path='" + path + '\'' +
                ", id='" + id + '\'' +
                ", resType=" + resType +
                ", header=" + Arrays.toString(header) +
                ", timeFormat='" + timeFormat + '\'' +
                ", schema='" + schema + '\'' +
                ", timeRange=" + timeRange +
                ", type='" + type + '\'' +
                ", fileInfoList=" + fileInfoList +
                '}';
    }
}
