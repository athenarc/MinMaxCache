package gr.imsi.athenarc.visual.middleware.datasource.influxdb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;

public class InfluxDBDataPointsIterator implements Iterator<DataPoint> {

    private final Integer numberOfTables;
    private int currentTable;
    private int currentSize;
    private int current;
    private List<FluxRecord> currentRecords;    
    private final Map<String, List<TimeInterval>> missingIntervalsPerMeasureName;
    Map<String, Integer> measuresMap;
    private final List<FluxTable> tables;

    public InfluxDBDataPointsIterator(Map<String, List<TimeInterval>> missingIntervalsPerMeasureName, Map<String, Integer> measuresMap, List<FluxTable> tables) {
        this.missingIntervalsPerMeasureName = missingIntervalsPerMeasureName;
        this.currentTable = 0;
        this.tables = tables;
        this.currentRecords = tables.get(currentTable).getRecords();
        this.currentSize = this.currentRecords.size();
        this.numberOfTables = tables.size();
        this.measuresMap = measuresMap;
        this.current = 0;
    }

    @Override
    public boolean hasNext() {
        if (current < currentSize) return true;
        else {
            if (currentTable < numberOfTables - 1) {
                current = 0;
                currentTable++;
                currentRecords = tables.get(currentTable).getRecords();
                currentSize = currentRecords.size();
                return true;
            } else return false;
        }
    }

    @Override
    public DataPoint next() {
        FluxRecord fluxRecord = tables.get(currentTable).getRecords().get(current++);
        String measure = Objects.requireNonNull(fluxRecord.getField());
        return new ImmutableDataPoint(Objects.requireNonNull(fluxRecord.getTime()).toEpochMilli(), (double) Objects.requireNonNull(fluxRecord.getValue()), measuresMap.get(measure));
    }
}