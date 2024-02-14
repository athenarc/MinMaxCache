package gr.imsi.athenarc.visual.middleware.index.csv;

import gr.imsi.athenarc.visual.middleware.domain.Dataset.CsvDataset;
import gr.imsi.athenarc.visual.middleware.index.TreeNode;

import java.util.DoubleSummaryStatistics;
import java.util.HashMap;

public class CsvTreeNode extends TreeNode {
    private long fileOffsetStart;
    private int dataPointCount = 0;

    public CsvTreeNode(int label, int level) {
        super(label, level);
    }

    public long getFileOffsetStart() {
        return fileOffsetStart;
    }

    public void setFileOffsetStart(long fileOffsetStart) {
        this.fileOffsetStart = fileOffsetStart;
    }

    public int getDataPointCount() {
        return dataPointCount;
    }

    public void setDataPointCount(int dataPointCount) {
        this.dataPointCount = dataPointCount;
    }

    public void adjustStats(String[] row, CsvDataset dataset) {
        if (statisticsMap == null) {
            statisticsMap = new HashMap<>();
        }
        for (int colIndex : dataset.getMeasures()) {
            DoubleSummaryStatistics statistics = statisticsMap.computeIfAbsent(colIndex, i -> new DoubleSummaryStatistics());
            statistics.accept(Double.parseDouble(row[colIndex]));
        }
    }

    @Override
    public String toString() {
        return "{" +
            " level='" + getLevel() + "'" +
            " label='" + getLabel() + "'" +
            " fileOffsetStart='" + getFileOffsetStart() + "'" +
            ", dataPointCount='" + getDataPointCount() + "'" +
            "}";
    }

    @Override

    public TreeNode createChild(int label, int level){
        return new CsvTreeNode(label, level);
    }
}
