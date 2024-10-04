package gr.imsi.athenarc.visual.middleware.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.datasource.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.InfluxDBQuery;
import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.SQLQuery;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.*;
import gr.imsi.athenarc.visual.middleware.domain.PostgreSQL.JDBCConnection;
import gr.imsi.athenarc.visual.middleware.domain.InfluxDB.InfluxDBConnection;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.domain.Query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.TimeInterval;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import gr.imsi.athenarc.visual.middleware.domain.ViewPort;
import gr.imsi.athenarc.visual.middleware.experiments.util.*;
import gr.imsi.athenarc.visual.middleware.util.io.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Experiments<T> {
    private static final Logger LOG = LoggerFactory.getLogger(Experiments.class);

    @Parameter(names = "-path", description = "The path of the input file(s)")
    public String path;

    @Parameter(names = "-queries", description = "The path of the input queries file if it exists")
    public String queries;

    @Parameter(names = "-type", description = "The type of the input")
    public String type;

    @Parameter(names = "-mode", description = "The mode of the experiment (tti/raw/influx/postgres")
    public String mode;

    @Parameter(names = "-measures", variableArity = true, description = "Measures IDs to be used")
    public List<Integer> measures;

    @Parameter(names = "-timeCol", description = "Datetime Column name")
    public String timeCol;

    @Parameter(names = "-idCol", description = "Measure name/id column name")
    public String idCol;

    @Parameter(names = "-valueCol", description = "Value Column name")
    public String valueCol;

    @Parameter(names = "-hasHeader", description = "If CSV has header")
    public Boolean hasHeader = true;

    @Parameter(names = "-timeFormat", description = "Datetime Column Format")
    public String timeFormat = "yyyy-MM-dd[ HH:mm:ss]";

    @Parameter(names = "-delimeter", description = "CSV Delimeter")
    public String delimiter = ",";


    @Parameter(names = "-zoomFactor", description = "Zoom factor for zoom in operation. The inverse applies to zoom out operation.")
    public Float zoomFactor = 0f;


    @Parameter(names = "-startTime", converter = EpochConverter.class, variableArity = true, description = "Start Time Epoch")
    Long startTime = 0L;

    @Parameter(names = "-endTime", converter = EpochConverter.class, variableArity = true, description = "End Time Epoch")
    Long endTime = 0L;

    @Parameter(names = "-q", description = "Query percent")
    Double q;

    @Parameter(names = "-p", description = "Prefetching factor")
    Double p;

    @Parameter(names = "-filters", converter = FilterConverter.class, description = "Q0 Filters")
    private HashMap<Integer, Double[]> filters = new HashMap<>();


    @Parameter(names = "-c", required = true)
    private String command;

    @Parameter(names = "-a")
    private float accuracy;

    @Parameter(names = "-agg")
    private int aggFactor;

    @Parameter(names = "-reduction")
    private int reductionFactor;

    @Parameter(names = "-out", description = "The output folder")
    private String outFolder;

    @Parameter(names = "-initMode")
    private String initMode;

    @Parameter(names = "-seqCount", description = "Number of queries in the sequence")
    private Integer seqCount;

    @Parameter(names = "-measureChange", description = "Number of times the measure changes")
    private Integer measureChange;

    @Parameter(names = "-objCount", description = "Number of objects")
    private Integer objCount;
    @Parameter(names = "-minShift", description = "Min shift in the query sequence")
    private Float minShift;
    @Parameter(names = "-maxShift", description = "Max shift in the query sequence")
    private Float maxShift;
    @Parameter(names = "-minFilters", description = "Min filters in the query sequence")
    private Integer minFilters;
    @Parameter(names = "-maxFilters", description = "Max filters in the query sequence")
    private Integer maxFilters;
    @Parameter(names = "-config", description = "PostgreSQL/InfluxDB config file path")
    private String config;
    @Parameter(names = "-schema", description = "PostgreSQL/InfluxDB schema name where data lay")
    private String schema;
    @Parameter(names = "-table", description = "PostgreSQL/InfluxDB table name to query")
    private String table;
    @Parameter(names = "-viewport", converter = ViewPortConverter.class, description = "Viewport of query")
    private ViewPort viewPort;
    @Parameter(names = "-runs", description = "Times to run each experiment workflow")
    private Integer runs;
    @Parameter(names = "--measureMem",  description = "Measure index memory after every query in the sequence")
    private boolean measureMem = false;

    @Parameter(names = "--help", help = true, description = "Displays help")
    private boolean help;



    public Experiments() {
   
    }


    public static void main(String... args) throws IOException, ClassNotFoundException, SQLException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments);
        jCommander.parse(args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    private void run() throws IOException, ClassNotFoundException, SQLException {
        Preconditions.checkNotNull(outFolder, "No out folder specified.");
        type = type.toLowerCase(Locale.ROOT);
        switch(type){
            case "postgres":
                if(config == null) config = "postgreSQL.cfg";
                break;
            case "influx":
                if(config == null) config = "influxDB.cfg";
                break;
            default:
                Preconditions.checkNotNull(outFolder, "No config files specified.");
        }
        initOutput();
        switch (command) {
            case "initialize":
                initialize();
                break;
            case "timeQueries":
                timeQueries();
                break;
            default:
        }
    }


    private void initialize() throws IOException, SQLException {
        AbstractDataset dataset = createInitDataset();
        QueryExecutor queryExecutor = createQueryExecutor(dataset);
        queryExecutor.drop();
        queryExecutor.initialize(path);
    }

    private void timeQueriesMinMaxCache(int run) throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "minMaxResults").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        QueryExecutor queryExecutor = createQueryExecutor(dataset);
        MinMaxCache minMaxCache = new MinMaxCache(queryExecutor, dataset, p, aggFactor, reductionFactor);
        QueryMethod queryMethod = QueryMethod.MIN_MAX;
        Query q0 = new Query(startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null);
        List<Query> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset", "query #", "operation", "width", "height", "from", "to", "timeRange", "aggFactor", "Results size", "IO Count",
                "Time (sec)", "Progressive Time (sec)", "Processing Time (sec)", "Query Time (sec)", "Memory", "Est. Raw Datapoints",
                "Error", "flag");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = (Query) sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i + " " + query.getOpType() + " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = minMaxCache.executeQuery(query);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            LOG.info("Query time: {}", time);
            long memorySize = minMaxCache.calculateDeepMemorySize();
            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(query.getViewPort().getWidth());
            csvWriter.addValue(query.getViewPort().getHeight());
            csvWriter.addValue(query.getFrom());
            csvWriter.addValue(query.getTo());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getAggFactors());
            csvWriter.addValue(0);
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(time);
            csvWriter.addValue(queryResults.getProgressiveQueryTime());
            csvWriter.addValue(time - queryResults.getQueryTime());
            csvWriter.addValue(queryResults.getQueryTime());
            csvWriter.addValue(memorySize);
            csvWriter.addValue((query.getTo() - query.getFrom()) / dataset.getSamplingInterval().toMillis()); // raw data points
//            csvWriter.addValue((double) ((query.getTo() - query.getFrom()) / queryResults.getAggFactor() / query.getViewPort().getWidth()) / dataset.getSamplingInterval().toMillis()); // data reduction factor
            csvWriter.addValue(queryResults.getError());
            csvWriter.addValue(queryResults.isFlag());
            csvWriter.writeValuesToRow();
            stopwatch.reset();
        }
        csvWriter.flush();
    }

    private void timeQueriesRawCache(int run) throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "rawResults").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        QueryExecutor queryExecutor = createQueryExecutor(dataset);
        MinMaxCache rawCache = new MinMaxCache(queryExecutor, dataset, p, aggFactor, Integer.MAX_VALUE); // If aggregationFactor is larger then the cache will always request raw data.
        QueryMethod queryMethod = QueryMethod.RAW;
        Query q0 = new Query(startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null);
        List<Query> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset", "query #", "operation", "width", "height", "from", "to", "timeRange", "Results size", "IO Count",  "Time (sec)", "Memory");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = (Query) sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i + " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = rawCache.executeQuery(query);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            LOG.info("Query time: {}", time);
            long memorySize = rawCache.calculateDeepMemorySize();
            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(viewPort.getWidth());
            csvWriter.addValue(viewPort.getHeight());
            csvWriter.addValue(query.getFrom());
            csvWriter.addValue(query.getTo());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(time);
            csvWriter.addValue(memorySize);
            csvWriter.writeValuesToRow();
            stopwatch.reset();

        }
        csvWriter.flush();
    }

    private void timeQueriesM4(int run) throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "m4Results").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        AbstractDataset dataset = createDataset();
        QueryExecutor queryExecutor = createQueryExecutor(dataset);
        QueryMethod queryMethod = QueryMethod.M4;
        Query q0 = new Query(startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null);
        List<Query> sequence = generateQuerySequence(q0, dataset);
        csvWriter.writeHeaders("dataset", "query #", "operation", "width", "height", "from", "to", "timeRange", "Results size", "Time (sec)");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i + " " + query.getFromDate() + " - " + query.getToDate());
            Map<Integer, List<TimeInterval>> missingTimeIntervalsPerMeasure = new HashMap<>(query.getMeasures().size());
            Map<String, List<TimeInterval>> missingTimeIntervalsPerMeasureName = new HashMap<>(query.getMeasures().size());
            Map<String, Integer> numberOfGroupsPerMeasureName = new HashMap<>(query.getMeasures().size());
            Map<Integer, Integer> numberOfGroups = new HashMap<>(query.getMeasures().size());

            for (Integer measure : query.getMeasures()) {
                String measureName = dataset.getHeader()[measure];
                List<TimeInterval> timeIntervalsForMeasure = new ArrayList<>();
                timeIntervalsForMeasure.add(new TimeRange(query.getFrom(), query.getTo()));
                missingTimeIntervalsPerMeasure.put(measure, timeIntervalsForMeasure);
                missingTimeIntervalsPerMeasureName.put(measureName, timeIntervalsForMeasure);

                numberOfGroups.put(measure, query.getViewPort().getWidth());
                numberOfGroupsPerMeasureName.put(measureName, query.getViewPort().getWidth());
            }
            DataSourceQuery dataSourceQuery = null;
            switch (type) {
                case "postgres":
                    dataSourceQuery = new SQLQuery(dataset.getSchema(), dataset.getTableName(), ((PostgreSQLDataset)dataset).getTimeCol(), ((PostgreSQLDataset)dataset).getIdCol(), ((PostgreSQLDataset)dataset).getValueCol(),
                            query.getFrom(), query.getTo(), missingTimeIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
                    break;
                case "influx":
                    dataSourceQuery = new InfluxDBQuery(dataset.getSchema(), dataset.getTableName(), query.getFrom(), query.getTo(), missingTimeIntervalsPerMeasureName, numberOfGroupsPerMeasureName);
                    break;
            }
            queryResults = queryExecutor.execute(dataSourceQuery, queryMethod);
            stopwatch.stop();
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getOpType());
            csvWriter.addValue(viewPort.getWidth());
            csvWriter.addValue(viewPort.getHeight());
            csvWriter.addValue(query.getFrom());
            csvWriter.addValue(query.getTo());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(queryResults.getData().get(this.measures.get(0)).size());
            csvWriter.addValue(time);
            csvWriter.writeValuesToRow();
            stopwatch.reset();
        }
        csvWriter.flush();
    }

    private void timeQueries() throws IOException, SQLException {
        Preconditions.checkNotNull(mode, "You must define the execution mode (tti, raw, postgres, influx).");
        for  (int i = 0; i < runs; i ++){
            Path runPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + i);
            FileUtil.build(runPath.toString());
            if(!mode.equals("all")) {
                Path path = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, mode + "Results");
                FileUtil.build(path.toString());
            }
            else {
                Path path1 = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, "minMax" + "Results");
                Path path2 = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, "m4" + "Results");
                Path path3 = Paths.get(outFolder, "timeQueries", type, table, "run_" + i, "raw" + "Results");
                FileUtil.build(path1.toString());
                FileUtil.build(path2.toString());
                FileUtil.build(path3.toString());
            }
        }
        for(int i = 0; i < runs; i ++) {
            switch (mode) {
                case "minMax":
                    timeQueriesMinMaxCache(i);
                    break;
                case "raw":
                    timeQueriesRawCache(i);
                    break;
                case "m4":
                    timeQueriesM4(i);
                    break;
                case "all":
                    timeQueriesMinMaxCache(i);
                    timeQueriesM4(i);
                    timeQueriesRawCache(i);
                    break;
                default:
                    System.exit(0);
            }
        }
    }


    private List<Query> generateQuerySequence(Query q0, AbstractDataset dataset) {
        Preconditions.checkNotNull(minShift, "Min query shift must be specified.");
        Preconditions.checkNotNull(maxShift, "Max query shift must be specified.");
        QuerySequenceGenerator sequenceGenerator = new QuerySequenceGenerator(minShift, maxShift, zoomFactor, dataset);
        if(queries != null) {
            Preconditions.checkNotNull(queries, "No given queries.txt file");
            return sequenceGenerator.generateQuerySequence(q0, queries);
        }
        else {
            Preconditions.checkNotNull(seqCount, "No sequence count specified.");
            return sequenceGenerator.generateQuerySequence(q0, seqCount, measureChange);
        }
    }

    private void initOutput() throws IOException {
        Path outFolderPath = Paths.get(outFolder);
        Path timeQueriesPath = Paths.get(outFolder, "timeQueries");
        Path typePath = Paths.get(outFolder, "timeQueries", type);
        Path tablePath = Paths.get(outFolder, "timeQueries", type, table);
        Path metadataPath = Paths.get("metadata");
        FileUtil.build(outFolderPath.toString());
        FileUtil.build(timeQueriesPath.toString());
        FileUtil.build(typePath.toString());
        FileUtil.build(tablePath.toString());
        FileUtil.build(metadataPath.toString());
    }


    private AbstractDataset createInitDataset() {
        AbstractDataset dataset = null;
        switch (type) {
            case "postgres":
                dataset = new PostgreSQLDataset(table, schema, table);
                break;
            case "influx":
                dataset = new InfluxDBDataset(table, schema, table);
                break;
            default:
                break;
        }
        return dataset;
    }
    
    private AbstractDataset createDataset() throws SQLException {
        String p = "";
        AbstractDataset dataset = null;
        switch (type) {
            case "postgres":
                p = String.valueOf(Paths.get("metadata", "postgres-" + table));
                if (new File(p).exists()) dataset = (PostgreSQLDataset) SerializationUtilities.loadSerializedObject(p);
                else{
                    JDBCConnection postgreSQLConnection =
                        (JDBCConnection) new JDBCConnection(config).connect();
                    dataset = new PostgreSQLDataset(postgreSQLConnection, table, schema, table);
                    SerializationUtilities.storeSerializedObject(dataset, p);
                }
                break;
            case "influx":
                p = String.valueOf(Paths.get("metadata", "influx-" + table));
                if (new File(p).exists()) dataset = (InfluxDBDataset) SerializationUtilities.loadSerializedObject(p);
                else {
                    InfluxDBConnection influxDBConnection = (InfluxDBConnection) new InfluxDBConnection(config).connect();
                    dataset = new InfluxDBDataset(influxDBConnection, table, schema, table);
                    SerializationUtilities.storeSerializedObject(dataset, p);
                }
                break;
            default:
                break;
        }
        LOG.info("Initialized Dataset: {}, range {}, header {}, sampling interval {}", dataset.getTableName(),
                dataset.getTimeRange(), Arrays.asList(dataset.getHeader()), dataset.getSamplingInterval());
        // If query percent given. Change start and end times based on it
        if(q != null){
            startTime = dataset.getTimeRange().getTo() - (long) (q * (dataset.getTimeRange().getTo() - dataset.getTimeRange().getFrom()));
            endTime = (dataset.getTimeRange().getTo());
        }
        return dataset;
    }

    private QueryExecutor createQueryExecutor(AbstractDataset dataset) throws IOException, SQLException {
        QueryExecutor queryExecutor = null;
        switch (type) {
            case "postgres":
                JDBCConnection postgreSQLConnection =
                        new JDBCConnection(config);
                postgreSQLConnection.connect();
                queryExecutor = postgreSQLConnection.getQueryExecutor(dataset);
                break;             
            case "influx":
                InfluxDBConnection influxDBConnection =
                        new InfluxDBConnection(config);
                influxDBConnection.connect();
                queryExecutor = influxDBConnection.getQueryExecutor(dataset);
            default:
                break;
        }
        return queryExecutor;
    }

}
