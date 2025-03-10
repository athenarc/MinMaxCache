package gr.imsi.athenarc.visual.middleware.datasource.iterator;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public abstract class InfluxDBIterator<T> implements Iterator<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(InfluxDBIterator.class);

    protected int current;
    protected long groupTimestamp;
    protected long currentGroupTimestamp;
    protected final Integer numberOfTables;
    protected int currentTable;
    protected int currentSize;
    protected List<FluxRecord> currentRecords;
    protected final List<FluxTable> tables;
    protected final Map<String, Integer> measuresMap;

    protected InfluxDBIterator(List<FluxTable> tables, Map<String, Integer> measuresMap) {
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("Tables list cannot be null or empty");
        }
        
        this.currentTable = 0;
        this.tables = tables;
        this.currentRecords = tables.get(0).getRecords();
        this.currentSize = this.currentRecords.size();
        this.numberOfTables = tables.size();
        this.current = 0;
        this.measuresMap = measuresMap;
        
        initializeTimestamps();
    }

    protected void initializeTimestamps() {
        if (!currentRecords.isEmpty()) {
            FluxRecord record = currentRecords.get(current);
            groupTimestamp = getTimestampFromRecord(record, "_start");
            currentGroupTimestamp = getTimestampFromRecord(record, "_stop");
        }
    }

    protected long getTimestampFromRecord(FluxRecord record, String field) {
        return ((Instant) record.getValues().get(field)).toEpochMilli();
    }

    @Override
    public boolean hasNext() {
        if (current < currentSize) return true;
        
        if (currentTable < numberOfTables - 1) {
            moveToNextTable();
            return true;
        }
        return false;
    }

    protected void moveToNextTable() {
        current = 0;
        currentTable++;
        currentRecords = tables.get(currentTable).getRecords();
        currentSize = currentRecords.size();
        initializeTimestamps();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements to iterate over");
        }
        return getNext();
    }

    protected abstract T getNext();
}
