package gr.imsi.athenarc.visual.middleware.datasource.csv;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;
/**
 * A simple iterator that returns ImmutableDataPoint objects for each measure
 * and only for timestamps within the measure's intervals.
 */
public class CsvDataPointsIterator implements Iterator<DataPoint> {

    private final Iterator<String[]> csvDataPointsIterator;
    private final Map<String, List<TimeInterval>> intervalsPerMeasure;       // Key: measure name, Value: intervals
    private final Map<String, Integer> measureColumnIndices;                // Key: measure name, Value: CSV column index
    private final int timeColumnIndex;
    private final DateTimeFormatter formatter;

    // A queue to hold DataPoints flattened from the current row
    private final Queue<DataPoint> nextDataPointsQueue = new LinkedList<>();

    public CsvDataPointsIterator(
            Iterable<String[]> csvDataPoints,
            Map<String, List<TimeInterval>> intervalsPerMeasure,
            Map<String, Integer> measureColumnIndices,
            int timeColumnIndex,
            DateTimeFormatter formatter
    ) {
        this.csvDataPointsIterator = csvDataPoints.iterator();
        this.intervalsPerMeasure = intervalsPerMeasure;
        this.measureColumnIndices = measureColumnIndices;
        this.timeColumnIndex = timeColumnIndex;
        this.formatter = formatter;
    }

    @Override
    public boolean hasNext() {
        // If we already have data points queued, we're good
        if (!nextDataPointsQueue.isEmpty()) {
            return true;
        }
        // Otherwise, read rows until we find DataPoints or we run out
        while (csvDataPointsIterator.hasNext()) {
            String[] row = csvDataPointsIterator.next();
            if (row.length <= timeColumnIndex) {
                continue; // skip invalid row
            }
            // Parse timestamp
            long timestamp = DateTimeUtil.parseDateTimeString(row[timeColumnIndex], formatter);

            // For each measure, check if we should produce a DataPoint
            for (Map.Entry<String, Integer> entry : measureColumnIndices.entrySet()) {
                String measureName = entry.getKey();
                int columnIndex = entry.getValue();
                if (row.length <= columnIndex) {
                    continue; // skip if row doesn't have the measure column
                }

                double value;
                try {
                    value = Double.parseDouble(row[columnIndex]);
                } catch (NumberFormatException e) {
                    continue; // skip invalid numeric data
                }

                // Check if the timestamp is in any of the intervals for this measure
                List<TimeInterval> intervals = intervalsPerMeasure.get(measureName);
                if (intervals == null) {
                    // If no intervals defined for this measure, skip
                    continue;
                }
                boolean withinInterval = intervals.stream().anyMatch(interval ->
                        timestamp >= interval.getFrom() && timestamp <= interval.getTo()
                );

                if (withinInterval) {
                    // Enqueue a new ImmutableDataPoint
                    nextDataPointsQueue.add(new ImmutableDataPoint(timestamp, value, columnIndex));
                }
            }

            // If the queue has data now, return
            if (!nextDataPointsQueue.isEmpty()) {
                return true;
            }
        }
        // No more rows, queue is empty
        return false;
    }

    @Override
    public DataPoint next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more CSV rows or intervals that match.");
        }
        return nextDataPointsQueue.poll();
    }
}