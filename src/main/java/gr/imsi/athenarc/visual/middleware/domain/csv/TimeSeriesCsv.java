package gr.imsi.athenarc.visual.middleware.domain.csv;

import java.io.Serializable;

import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

public class TimeSeriesCsv implements TimeInterval, Serializable {

    private long from;
    private long to;
    private String filePath;


    public TimeSeriesCsv(long from, long to, String filePath) {
        this.from = from;
        this.to = to;
        this.filePath = filePath;
    }

    public TimeSeriesCsv(TimeInterval fileTimeRange, String filePath) {
        this.from = fileTimeRange.getFrom();
        this.to = fileTimeRange.getTo();
        this.filePath = filePath;
    }

    @Override
    public long getFrom() {
        return from;
    }

    @Override
    public long getTo() {
        return to;
    }

    public String getFilePath() {
        return filePath;
    }

    public TimeInterval getTimeRange() {
        return new TimeRange(from, to);
    }
}
