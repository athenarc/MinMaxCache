package gr.imsi.athenarc.visual.middleware.domain.Dataset;

import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import gr.imsi.athenarc.visual.middleware.domain.InfluxDB.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.InfluxDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class InfluxDBDataset extends AbstractDataset {

    private String influxDBCfg;
    private final String bucket;
    private final String measurement;
    private final String timeFormat;

    public InfluxDBDataset(String influxDBCfg, String id, String bucket, String measurement, String timeFormat) {
        super(id);
        this.influxDBCfg = influxDBCfg;
        this.bucket = bucket;
        this.measurement = measurement;
        this.timeFormat = timeFormat;
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(influxDBCfg);
    }

    public InfluxDBDataset(InfluxDBQueryExecutor influxDBQueryExecutor, String id, String bucket,
                           String measurement, String timeFormat, String timeCol) {
        super(id);
        this.bucket = bucket;
        this.measurement = measurement;
        this.timeFormat = timeFormat;
        setTimeCol(timeCol);
        this.fillInfluxDBDatasetInfo(influxDBQueryExecutor);
    }



    public InfluxDBDataset(String influxDBCfg, String id, String bucket,
                           String measurement, String timeFormat, String timeCol) {
        super(id);
        this.influxDBCfg = influxDBCfg;
        this.bucket = bucket;
        this.measurement = measurement;
        this.timeFormat = timeFormat;
        setTimeCol(timeCol);
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(influxDBCfg);
        influxDBConnection.connect();
        this.fillInfluxDBDatasetInfo(influxDBConnection.getQueryExecutor());
    }



    private void fillInfluxDBDatasetInfo(InfluxDBQueryExecutor influxDBQueryExecutor) {
        List<FluxTable> fluxTables;
        String firstQuery = "from(bucket:\"" + bucket + "\")\n" +
                "  |> range(start: 1970-01-01T00:00:00.000Z, stop: 2150-01-01T00:00:00.999Z)\n" +
                "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + measurement + "\")\n" +
                "  |> limit(n: 2)\n" +
                "  |> yield(name:\"first\")\n";
        fluxTables = influxDBQueryExecutor.execute(firstQuery);

        Set<String> header = new LinkedHashSet<>();
        long from = Long.MAX_VALUE;
        long second = 0L;

        for(FluxTable fluxTable : fluxTables) {
            int i = 0;
            for (FluxRecord record : fluxTable.getRecords()) {
                if (i == 1) second = Objects.requireNonNull(record.getTime()).toEpochMilli();
                header.add(record.getField());
                long time = Objects.requireNonNull(record.getTime()).toEpochMilli();
                from = Math.min(from, time);
                i++;
            }
        }
        
        String lastQuery =  "from(bucket:\"" + bucket + "\")\n" +
                "  |> range(start: 0, stop:2120-01-01T00:00:00.000Z)\n" +
                "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + measurement + "\")\n" +
                "  |> keep(columns: [\"_time\"])\n" +
                "  |> last(column: \"_time\")\n";

        fluxTables = influxDBQueryExecutor.execute(lastQuery);
        FluxRecord record = fluxTables.get(0).getRecords().get(0);
        long to = record.getTime().toEpochMilli();

        setSamplingInterval(Duration.of(second - from, ChronoUnit.MILLIS));
        setTimeRange(new TimeRange(from, to));
        setHeader(header.toArray(new String[0]));
    }

    @Override
    public String getTimeFormat() {
        return timeFormat;
    }

    @Override
    public String getSchema() {
        return bucket;
    }

    @Override
    public String getTable() {
        return measurement;
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

    public String getConfig(){
        return influxDBCfg;
    }

    @Override
    public String toString() {
        return "InfluxDBDataset{" +
                "fileInfoList=" + fileInfoList +
                ", influxDBCfg='" + influxDBCfg + '\'' +
                ", bucket='" + bucket + '\'' +
                ", measurement='" + measurement + '\'' +
                ", timeFormat='" + timeFormat + '\'' +
                '}';
    }
}
