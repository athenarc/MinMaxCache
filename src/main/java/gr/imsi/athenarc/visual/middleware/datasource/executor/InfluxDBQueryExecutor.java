package gr.imsi.athenarc.visual.middleware.datasource.executor;

import com.influxdb.client.DeleteApi;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

import gr.imsi.athenarc.visual.middleware.cache.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.InfluxDBDataset;
import gr.imsi.athenarc.visual.middleware.datasource.query.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.query.InfluxDBQuery;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.ImmutableDataPoint;
import gr.imsi.athenarc.visual.middleware.domain.influxdb.InitQueries.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class InfluxDBQueryExecutor implements QueryExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBQueryExecutor.class);

    InfluxDBClient influxDBClient;
    InfluxDBDataset dataset;

    String table;
    String bucket;
    String org;

    public InfluxDBQueryExecutor(InfluxDBClient influxDBClient, String bucket, String org){
        this.influxDBClient = influxDBClient;
        this.bucket = bucket;
        this.org = org;
    }
    public InfluxDBQueryExecutor(InfluxDBClient influxDBClient, AbstractDataset dataset, String org) {
        this.influxDBClient = influxDBClient;
        this.dataset = (InfluxDBDataset) dataset;
        this.table = dataset.getTableName();
        this.bucket = dataset.getSchema();
        this.org = org;
    }

    @Override
    public Map<Integer, List<DataPoint>> execute(DataSourceQuery q, QueryMethod method) {
        switch (method) {
            case M4:
                return executeM4Query(q);
            case RAW:
                return executeRawQuery(q);
            case MIN_MAX:
                return executeMinMaxQuery(q);
            default:
                return executeM4Query(q);
        }
    }

    @Override
    public Map<Integer, List<DataPoint>> executeM4Query(DataSourceQuery q) {
        return collect(executeM4InfluxQuery((InfluxDBQuery) q));
    }

    @Override
    public Map<Integer, List<DataPoint>> executeRawQuery(DataSourceQuery q) {
        return collect(executeRawInfluxQuery((InfluxDBQuery) q));
    }

    @Override
    public Map<Integer, List<DataPoint>> executeMinMaxQuery(DataSourceQuery q) {return collect(executeMinMaxInfluxQuery((InfluxDBQuery) q));}

    @Override
    public void initialize(String path) throws FileNotFoundException {
        WriteApi writeApi = influxDBClient.makeWriteApi();
        FileReader reader;
        switch (table) {
            case "bebeze": {
                reader = new FileReader(path);
                CsvToBean<BEBEZE> csvToBean = new CsvToBeanBuilder<BEBEZE>(reader)
                        .withType(BEBEZE.class)
                        .build();
                for (BEBEZE data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "intel_lab": {
                reader = new FileReader(path);
                CsvToBean<INTEL_LAB> csvToBean = new CsvToBeanBuilder<INTEL_LAB>(reader)
                        .withType(INTEL_LAB.class)
                        .build();
                for (INTEL_LAB data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "soccer": {
                reader = new FileReader(path);
                CsvToBean<SOCCER> csvToBean = new CsvToBeanBuilder<SOCCER>(reader)
                        .withType(SOCCER.class)
                        .build();
                for (SOCCER data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "manufacturing": {
                reader = new FileReader(path);
                CsvToBean<MANUFACTURING> csvToBean = new CsvToBeanBuilder<MANUFACTURING>(reader)
                        .withType(MANUFACTURING.class)
                        .build();
                for (MANUFACTURING data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "intel_lab_exp": {
                reader = new FileReader(path);
                CsvToBean<INTEL_LAB_EXP> csvToBean = new CsvToBeanBuilder<INTEL_LAB_EXP>(reader)
                        .withType(INTEL_LAB_EXP.class)
                        .build();
                for (INTEL_LAB_EXP data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "soccer_exp": {
                reader = new FileReader(path);
                CsvToBean<SOCCER_EXP> csvToBean = new CsvToBeanBuilder<SOCCER_EXP>(reader)
                        .withType(SOCCER_EXP.class)
                        .build();
                for (SOCCER_EXP data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "manufacturing_exp": {
                reader = new FileReader(path);
                CsvToBean<MANUFACTURING_EXP> csvToBean = new CsvToBeanBuilder<MANUFACTURING_EXP>(reader)
                        .withType(MANUFACTURING_EXP.class)
                        .build();
                for (MANUFACTURING_EXP data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic1m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC1M> csvToBean = new CsvToBeanBuilder<SYNTHETIC1M>(reader)
                        .withType(SYNTHETIC1M.class)
                        .build();
                for (SYNTHETIC1M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic2m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC2M> csvToBean = new CsvToBeanBuilder<SYNTHETIC2M>(reader)
                        .withType(SYNTHETIC2M.class)
                        .build();
                for (SYNTHETIC2M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic4m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC4M> csvToBean = new CsvToBeanBuilder<SYNTHETIC4M>(reader)
                        .withType(SYNTHETIC4M.class)
                        .build();
                for (SYNTHETIC4M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic8m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC8M> csvToBean = new CsvToBeanBuilder<SYNTHETIC8M>(reader)
                        .withType(SYNTHETIC8M.class)
                        .build();
                for (SYNTHETIC8M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic16m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC16M> csvToBean = new CsvToBeanBuilder<SYNTHETIC16M>(reader)
                        .withType(SYNTHETIC16M.class)
                        .build();
                for (SYNTHETIC16M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic32m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC32M> csvToBean = new CsvToBeanBuilder<SYNTHETIC32M>(reader)
                        .withType(SYNTHETIC32M.class)
                        .build();
                for (SYNTHETIC32M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic64m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC64M> csvToBean = new CsvToBeanBuilder<SYNTHETIC64M>(reader)
                        .withType(SYNTHETIC64M.class)
                        .build();
                for (SYNTHETIC64M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic128m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC128M> csvToBean = new CsvToBeanBuilder<SYNTHETIC128M>(reader)
                        .withType(SYNTHETIC128M.class)
                        .build();
                for (SYNTHETIC128M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic256m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC256M> csvToBean = new CsvToBeanBuilder<SYNTHETIC256M>(reader)
                        .withType(SYNTHETIC256M.class)
                        .build();
                for (SYNTHETIC256M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic512m": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC512M> csvToBean = new CsvToBeanBuilder<SYNTHETIC512M>(reader)
                        .withType(SYNTHETIC512M.class)
                        .build();
                for (SYNTHETIC512M data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
            case "synthetic1b": {
                reader = new FileReader(path);
                CsvToBean<SYNTHETIC1B> csvToBean = new CsvToBeanBuilder<SYNTHETIC1B>(reader)
                        .withType(SYNTHETIC1B.class)
                        .build();
                for (SYNTHETIC1B data : csvToBean) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.MS, data);
                }
                break;
            }
        }
        influxDBClient.close();
    }

    @Override
    public void drop() {
        OffsetDateTime start = OffsetDateTime.of(LocalDateTime.of(1970, 1, 1,
                0, 0, 0), ZoneOffset.UTC);
        OffsetDateTime stop = OffsetDateTime.now();
        String predicate = "_measurement=" + table;
        DeleteApi deleteApi = influxDBClient.getDeleteApi();
        deleteApi.delete(start, stop, predicate, bucket, org);
    }


    Comparator<DataPoint> compareLists = new Comparator<DataPoint>() {
        @Override
        public int compare(DataPoint s1, DataPoint s2) {
            if (s1 == null && s2 == null) return 0;//swapping has no point here
            if (s1 == null) return 1;
            if (s2 == null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };


    public List<FluxTable> executeM4InfluxQuery(InfluxDBQuery q) {
        String flux = q.m4QuerySkeleton();
        return executeDbQuery(flux);
    }


    public List<FluxTable> executeMinMaxInfluxQuery(InfluxDBQuery q) {
        String flux = q.minMaxQuerySkeleton();
        return executeDbQuery(flux);
    }


    public List<FluxTable> executeRawInfluxQuery(InfluxDBQuery q){
        String flux = q.rawQuerySkeleton();
        return executeDbQuery(flux);
    }

    private Map<Integer, List<DataPoint>> collect(List<FluxTable> tables) {
        HashMap<Integer, List<DataPoint>> data = new HashMap<>();
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                Integer fieldId = Arrays.asList(dataset.getHeader()).indexOf(fluxRecord.getField());
                data.computeIfAbsent(fieldId, k -> new ArrayList<>()).add(
                        new ImmutableDataPoint(Objects.requireNonNull(fluxRecord.getTime()).toEpochMilli(),
                                Double.parseDouble(Objects.requireNonNull(fluxRecord.getValue()).toString()), fieldId));
            }
        }
        data.forEach((k, v) -> v.sort(Comparator.comparingLong(DataPoint::getTimestamp)));
        return data;
    }

    public List<FluxTable> executeDbQuery(String query) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        LOG.info("Executing Query: \n" + query);
        return queryApi.query(query);
    }


    public Map<Integer, List<DataPoint>> execute(String query) {
        return collect(executeDbQuery(query));
    }
}