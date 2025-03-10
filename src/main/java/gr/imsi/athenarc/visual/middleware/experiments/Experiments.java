package gr.imsi.athenarc.visual.middleware.experiments;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import gr.imsi.athenarc.visual.middleware.cache.MinMaxCache;
import gr.imsi.athenarc.visual.middleware.cache.query.Query;
import gr.imsi.athenarc.visual.middleware.cache.query.QueryResults;
import gr.imsi.athenarc.visual.middleware.datasource.DataSource;
import gr.imsi.athenarc.visual.middleware.datasource.DataSourceFactory;
import gr.imsi.athenarc.visual.middleware.datasource.config.CsvConfiguration;
import gr.imsi.athenarc.visual.middleware.datasource.config.DataSourceConfiguration;
import gr.imsi.athenarc.visual.middleware.datasource.config.InfluxDBConfiguration;
import gr.imsi.athenarc.visual.middleware.datasource.config.PostgreSQLConfiguration;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.*;
import gr.imsi.athenarc.visual.middleware.datasource.query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.domain.ViewPort;
import gr.imsi.athenarc.visual.middleware.experiments.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
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
    Double prefetchingFactor;

    @Parameter(names = "-filters", converter = FilterConverter.class, description = "Q0 Filters")
    private HashMap<Integer, Double[]> filters = new HashMap<>();
    
    @Parameter(names = "-a")
    private float accuracy;

    @Parameter(names = "-agg")
    private int aggFactor;

    @Parameter(names = "-reduction")
    private int reductionFactor;

    @Parameter(names = "-out", description = "The output folder")
    private String outFolder;

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


    public static void main(String... args) throws IOException, SQLException, NoSuchMethodException {
        Experiments experiments = new Experiments();
        JCommander jCommander = new JCommander(experiments);
        jCommander.parse(args);
        if (experiments.help) {
            jCommander.usage();
        } else {
            experiments.run();
        }
    }

    private void run() throws IOException, SQLException, NoSuchMethodException {
        Preconditions.checkNotNull(outFolder, "No out folder specified.");
        type = type.toLowerCase(Locale.ROOT);
        initOutput();
        timeQueries();
    }

    private void timeQueriesMinMaxCache(int run) throws IOException, SQLException {
        String resultsPath = Paths.get(outFolder, "timeQueries", type, table, "run_" + run, "minMaxResults").toString();
        File outFile = Paths.get(resultsPath, "results.csv").toFile();
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        CsvWriter csvWriter = new CsvWriter(new FileWriter(outFile, false), csvWriterSettings);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        DataSource dataSource = createDatasource();
        MinMaxCache minMaxCache = new MinMaxCache(dataSource, prefetchingFactor, aggFactor, reductionFactor);
        QueryMethod queryMethod = QueryMethod.MIN_MAX;
        Query q0 = initiliazeQ0(dataSource.getDataset(), startTime, endTime, accuracy, null, queryMethod, measures, viewPort, null );
        List<Query> sequence = generateQuerySequence(q0, dataSource.getDataset());
        csvWriter.writeHeaders("dataset", "query #", "width", "height", "from", "to", "timeRange", "Results size", "IO Count",
                "Time (sec)", "Progressive Time (sec)", "Processing Time (sec)", "Query Time (sec)", "Memory");
        for (int i = 0; i < sequence.size(); i += 1) {
            stopwatch.start();
            Query query = (Query) sequence.get(i);
            QueryResults queryResults;
            double time = 0;
            LOG.info("Executing query " + i + " " + query.getFromDate() + " - " + query.getToDate());
            queryResults = minMaxCache.executeQuery(query);
            time = stopwatch.elapsed(TimeUnit.NANOSECONDS) / Math.pow(10d, 9);
            LOG.info("Query time: {}", time);
            long memorySize = minMaxCache.calculateDeepMemorySize();
            if(run == 0) queryResults.toMultipleCsv(Paths.get(resultsPath, "query_" + i).toString());
            csvWriter.addValue(table);
            csvWriter.addValue(i);
            csvWriter.addValue(query.getViewPort().getWidth());
            csvWriter.addValue(query.getViewPort().getHeight());
            csvWriter.addValue(query.getFrom());
            csvWriter.addValue(query.getTo());
            csvWriter.addValue(query.getFromDate() + " - " + query.getToDate());
            csvWriter.addValue(0);
            csvWriter.addValue(queryResults.getIoCount());
            csvWriter.addValue(time);
            csvWriter.addValue(queryResults.getProgressiveQueryTime());
            csvWriter.addValue(time - queryResults.getQueryTime());
            csvWriter.addValue(queryResults.getQueryTime());
            csvWriter.addValue(memorySize);
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
        FileUtil.build(outFolderPath.toString());
        FileUtil.build(timeQueriesPath.toString());
        FileUtil.build(typePath.toString());
        FileUtil.build(tablePath.toString());
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

    private DataSource createDatasource(){
        DataSource datasource = null;
        DataSourceConfiguration dataSourceConfiguration = null;
        Properties properties = readProperties();
        LOG.info("{}", properties);
        switch (type) {
            case "csv":
                dataSourceConfiguration = new CsvConfiguration(path, table, timeFormat, timeCol, delimiter, hasHeader);
                break;
            case "postgres":
                dataSourceConfiguration = new PostgreSQLConfiguration.Builder()
                    .url(properties.getProperty("postgres.url"))
                    .username(properties.getProperty("postgres.username"))
                    .password(properties.getProperty("postgres.password"))
                    .schema(schema)
                    .timeFormat(timeFormat)
                    .table(table)
                    .build();  
                break;
            case "influx":
                dataSourceConfiguration = new InfluxDBConfiguration.Builder()
                    .url(properties.getProperty("influxdb.url"))
                    .org(properties.getProperty("influxdb.org"))
                    .token(properties.getProperty("influxdb.token"))
                    .bucket(schema)
                    .timeFormat(timeFormat)
                    .measurement(table)
                    .build();   
                break;
            default:
                break;
            
        }
        datasource = DataSourceFactory.createDataSource(dataSourceConfiguration);
        return datasource;
    }

    private Query initiliazeQ0(AbstractDataset dataset, Long startTime, Long endTime, float accuracy, HashMap<Integer, Double[]> filters, QueryMethod queryMethod, List<Integer> measures, ViewPort viewPort, UserOpType opType){
        // If query percent given. Change start and end times based on it
        if(q != null){
            startTime = dataset.getTimeRange().getTo() - (long) (q * (dataset.getTimeRange().getTo() - dataset.getTimeRange().getFrom()));
            endTime = (dataset.getTimeRange().getTo());
        }
        return new Query(startTime, endTime, measures, viewPort.getWidth(), viewPort.getHeight(), accuracy);
    }


    public static Properties readProperties(){
        Properties properties = new Properties();
        // Load from the resources folder
        // "/application.properties" assumes the file is at src/main/resources/application.properties
        try (InputStream input = Experiments.class.getResourceAsStream("/application.properties")) {
            if (input == null) {
                LOG.error("Sorry, unable to find application.properties in resources.");
                return null;
            }
            properties.load(input);
            
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return properties;
    }
    
}
