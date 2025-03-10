package gr.imsi.athenarc.visual.middleware.cache.query;
import com.opencsv.CSVWriter;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;

public class QueryResults implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<Integer, List<DataPoint>> data;

    private Map<Integer, DoubleSummaryStatistics> measureStats;

    private TimeInterval timeRange;

    private long ioCount = 0;

    private double queryTime = 0;

    private double progressiveQueryTime = 0;


    public TimeInterval getTimeRange() {
        return this.timeRange;
    }

    public void setTimeRange(TimeInterval timeRange) {
        this.timeRange = timeRange;
    }

    public Map<Integer, List<DataPoint>> getData() {
        return data;
    }

    public void setData(Map<Integer, List<DataPoint>> data) {
        this.data = data;
    }

    public Map<Integer, DoubleSummaryStatistics> getMeasureStats() {
        return measureStats;
    }

    public void setMeasureStats(Map<Integer, DoubleSummaryStatistics> measureStats) {
        this.measureStats = measureStats;
    }
    
    public long getIoCount() {
        return ioCount;
    }

    public void setIoCount(long ioCount) {
        this.ioCount = ioCount;
    }

    public void toCsv(String path) {
        File file = new File(path);
        try {
            // create FileWriter object with file as parameter
            FileWriter outputFile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputFile);

            // adding header to csv
            int index = 1;
            int noOfRows = 0;
            int noOfCols = data.size();
            String[] header = new String[getData().size() + 1];
            header[0] = "timestamp";
            for (Map.Entry<Integer, List<DataPoint>> mapEntry : getData().entrySet()) {
                header[index] = String.valueOf(mapEntry.getKey());
                index++;
                noOfRows = Math.max(noOfRows, mapEntry.getValue().size());
            }
            writer.writeNext(header);
            // add data to csv
            String[][] rows = new String[noOfRows][noOfCols + 1];
            int col = 1;
            for (Map.Entry<Integer, List<DataPoint>> mapEntry : getData().entrySet()) {
                List<DataPoint> dataPoints = mapEntry.getValue();
                int row = 0;
                for (DataPoint dataPoint : dataPoints) {
                    rows[row][0] = String.valueOf(dataPoint.getTimestamp());
                    rows[row][col] = String.valueOf(dataPoint.getValue());
                    row++;
                }
                col++;
            }
            for (int row = 0; row < noOfRows; row++)
                writer.writeNext(rows[row], false);
            // closing writer connection
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void toMultipleCsv(String path) {
        try {
            File theDir = new File(path);
            if (!theDir.exists()) {
                theDir.mkdirs();
            }
            for (Map.Entry<Integer, List<DataPoint>> mapEntry : getData().entrySet()) {
                Integer measure = mapEntry.getKey();
                List<DataPoint> dataPoints = mapEntry.getValue();
                int noOfRows = dataPoints.size();
                Path filePath = Paths.get(theDir.getPath(), measure.toString() + ".csv");
                FileWriter outputFile = new FileWriter(filePath.toString());

                // create CSVWriter object filewriter object as parameter
                CSVWriter writer = new CSVWriter(outputFile);

                String[] header = {"timestamp", measure.toString()};
                writer.writeNext(header);

                String[][] rows = new String[noOfRows][2];
                int row = 0;
                for (DataPoint dataPoint : dataPoints) {
                    rows[row][0] = String.valueOf(dataPoint.getTimestamp());
                    rows[row][1] = String.valueOf(dataPoint.getValue());
                    row++;
                }

                for (row = 0; row < noOfRows; row++)
                    writer.writeNext(rows[row], false);
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public double getQueryTime() {
        return queryTime;
    }

    public void setQueryTime(double queryTime) {
        this.queryTime = queryTime;
    }

    public double getProgressiveQueryTime() {
        return progressiveQueryTime;
    }

    public void setProgressiveQueryTime(double progressiveQueryTime) {
        this.progressiveQueryTime = progressiveQueryTime;
    }

    @Override
    public String toString() {
        return "QueryResults{" +
                "data=" + data +
                ", measureStats=" + measureStats +
                ", timeRange=" + timeRange +
                ", ioCount=" + ioCount +
                '}';
    }
}
