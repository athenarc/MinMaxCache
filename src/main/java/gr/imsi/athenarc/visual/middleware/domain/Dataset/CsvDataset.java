package gr.imsi.athenarc.visual.middleware.domain.Dataset;

import gr.imsi.athenarc.visual.middleware.domain.DataFileInfo;
import gr.imsi.athenarc.visual.middleware.domain.MeasureStats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * A CSV Dataset.
 */
public class CsvDataset extends AbstractDataset implements Serializable {

    private static final long serialVersionUID = 1L;
    public String delimiter;
    private String timeCol;
    private Boolean hasHeader;
    private List<Integer> measures = new ArrayList<>();
    private Map<Integer, MeasureStats> measureStats;
    List<DataFileInfo> fileInfoList = new ArrayList<>();

    

    public CsvDataset(String path, String id, String table, String timeCol, String timeFormat, String delimiter, Boolean hasHeader) {
        super(path, id, table);
        this.timeCol = timeCol;
        this.delimiter = delimiter;
        this.hasHeader = hasHeader;
    }

    public Boolean getHasHeader() {
        return hasHeader;
    }

    public void setHasHeader(Boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public void setMeasures(List<Integer> measures) {
        this.measures = measures;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public Map<Integer, MeasureStats> getMeasureStats() {
        return measureStats;
    }

    public void setMeasureStats(Map<Integer, MeasureStats> measureStats) {
        this.measureStats = measureStats;
    }


    public String getTimeCol() {
        return timeCol;
    }

    public void setTimeCol(String timeCol) {
        this.timeCol = timeCol;
    }

    public List<DataFileInfo> getFileInfoList() {
        return fileInfoList;
    }

    public void setFileInfoList(List<DataFileInfo> fileInfoList) {
        this.fileInfoList = fileInfoList;
    }

    public int getMeasureIndex(String measure){
        return IntStream.range(0, getHeader().length)
            .filter(i -> getHeader()[i].equals(getTimeCol()))
            .findFirst()
            .orElse(-1);
    }


}
