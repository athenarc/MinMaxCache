package gr.imsi.athenarc.visual.middleware.datasource.dataset;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;

import gr.imsi.athenarc.visual.middleware.datasource.connector.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.datasource.executor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBDataset extends AbstractDataset {

    private static String DEFAULT_INFLUX_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBDataset.class);


    public InfluxDBDataset(){}
    // Abstract class implementation
    public InfluxDBDataset(String id, String bucket, String measurement){
        super(id, bucket, measurement, DEFAULT_INFLUX_FORMAT);
    }
    
    public InfluxDBDataset(InfluxDBConnection influxDBConnection, String id, String bucket, String measurement) {
        super(id, bucket, measurement, DEFAULT_INFLUX_FORMAT);
        this.fillInfluxDBDatasetInfo(influxDBConnection.getQueryExecutor(this));
    }
    
    private void fillInfluxDBDatasetInfo(InfluxDBQueryExecutor influxDBQueryExecutor) {
        // Fetch first timestamp
        String firstQuery = "from(bucket:\"" + getSchema() + "\")\n" +
            "  |> range(start: 1970-01-01T00:00:00.000Z, stop: now())\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + getTableName() + "\")\n" +
            "  |> first()\n";
    
        List<FluxTable> fluxTables = influxDBQueryExecutor.execute(firstQuery);
        FluxRecord firstRecord = fluxTables.get(0).getRecords().get(0);
        long firstTime = firstRecord.getTime().toEpochMilli();
    
        // Fetch last timestamp
        String lastQuery = "from(bucket:\"" + getSchema() + "\")\n" +
            "  |> range(start: 1970-01-01T00:00:00.000Z, stop: now())\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + getTableName() + "\")\n" +
            "  |> last()\n";
    
        fluxTables = influxDBQueryExecutor.execute(lastQuery);
        FluxRecord lastRecord = fluxTables.get(0).getRecords().get(0);
        long lastTime = lastRecord.getTime().toEpochMilli();
    
        String influxFormat = "\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"";
        // Fetch the second timestamp to calculate the sampling interval.
        // Query on first time plus some time later
        String secondQuery = "from(bucket:\"" + getSchema() + "\")\n" +
            "  |> range(start:" + DateTimeUtil.format(influxFormat, firstTime).replace("\"", "") + ", stop: " + DateTimeUtil.format(influxFormat, firstTime + 60000).replace("\"", "") + ")\n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + getTableName() + "\")\n" +
            "  |> limit(n: 2)\n";  // Fetch the first two records
    
        fluxTables = influxDBQueryExecutor.execute(secondQuery);
        FluxRecord secondRecord = fluxTables.get(0).getRecords().get(1); 
        long secondTime = secondRecord.getTime().toEpochMilli();
    
        // Calculate and set sampling interval
        setSamplingInterval(secondTime - firstTime);
    
        // Set time range and headers
        setTimeRange(new TimeRange(firstTime, lastTime));
    
        // Populate header (field keys)
        Set<String> header = fluxTables.stream()
            .flatMap(fluxTable -> fluxTable.getRecords().stream())
            .map(FluxRecord::getField)
            .collect(Collectors.toSet());
        
        setHeader(header.toArray(new String[0]));
        LOG.info("Dataset: {}, {}", this.getTimeRange(), this.getSamplingInterval());
    }


    @Override
    public List<Integer> getMeasures() {
        int[] measures = new int[getHeader().length];
        for(int i = 0; i < measures.length; i++)
            measures[i] = i;
        return Arrays.stream(measures)
                .boxed()
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "InfluxDBDataset{" +
                ", bucket='" + getSchema() + '\'' +
                ", measurement='" + getTableName() + '\'' +
                '}';
    }
}
