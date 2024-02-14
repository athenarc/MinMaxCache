package gr.imsi.athenarc.visual.middleware.domain.Dataset;

import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.ModelarDBQueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.ModelarDB.ModelarDBConnection;
import gr.imsi.athenarc.visual.middleware.domain.TimeRange;
import cfjd.org.apache.arrow.flight.FlightStream;
import cfjd.org.apache.arrow.vector.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ModelarDBDataset extends AbstractDataset {
    private String config;
    public ModelarDBDataset(ModelarDBQueryExecutor modelarDBQueryExecutor, String id, String schema, String table,
                            String timeFormat, String timeCol, String idCol, String valueCol)  {
        super(id, table, schema, timeFormat, timeCol, idCol, valueCol);
        this.fillModelarDBDatasetInfo(modelarDBQueryExecutor);
    }

    public ModelarDBDataset(String config, String id, String schema, String table,
                            String timeFormat, String timeCol, String idCol, String valueCol)  {
        super(id, table, schema, timeFormat, timeCol, idCol, valueCol);
        ModelarDBQueryExecutor modelarDBQueryExecutor =
                new ModelarDBConnection(config).getSqlQueryExecutor();
        this.config = config;
        this.fillModelarDBDatasetInfo(modelarDBQueryExecutor);
    }

    public ModelarDBDataset(String config, String table, String schema, String table1, String timeFormat) {
    }

    private void fillModelarDBDatasetInfo(ModelarDBQueryExecutor modelarDBQueryExecutor)  {
        try {
            String timeCol = getTimeCol();
            String idCol = getIdCol();
            String valueCol = getValueCol();
            String table = getTable();
            String timeFormat = getTimeFormat();

            // Header query
            String headerQuery = "SELECT DISTINCT(" + idCol + ") FROM " + table;
            FlightStream flightStream = modelarDBQueryExecutor.execute(headerQuery);
            List<String> header = new ArrayList<>();
            VectorSchemaRoot vsr = flightStream.getRoot();
            while(flightStream.next()){
                for (int i = 0 ; i < vsr.getRowCount(); i ++) {
                    byte[] bytes = ((VarCharVector) vsr.getVector(idCol)).get(i);
                    String s = new String(bytes);
                    if (s.equals(timeCol)) continue;
                    header.add(s);
                }
            }
            String[] headerArr = header.toArray(new String[0]);
            Arrays.sort(headerArr);
            setHeader(headerArr);

            // First date and sampling frequency query
            String firstQuery = "SELECT\n" + timeCol + " \n" +
                    "FROM " + table + " \n" +
                    "WHERE " + idCol + " = " + "'" + getHeader()[0] + "'" + " \n" +
                    "ORDER BY " + timeCol + " ASC\n" +
                    "LIMIT 2";
            flightStream = modelarDBQueryExecutor.execute(firstQuery);
            vsr = flightStream.getRoot();
            flightStream.next();
            long from = ((TimeStampMilliVector) vsr.getVector(timeCol)).get(0);
            flightStream.next();
            ((TimeStampMilliVector) vsr.getVector(timeCol)).get(1);
            long second = ((TimeStampMilliVector) vsr.getVector(timeCol)).get(1);
            // Last date query
            String lastQuery = "SELECT\n" + getTimeCol() + " \n" +
                    "FROM " + table + "\n" +
                    "WHERE " + idCol + " = " + "'" + getHeader()[0] + "'" + " \n" +
                    "ORDER BY " + getTimeCol() + " DESC\n" +
                    "LIMIT 1";
            flightStream = modelarDBQueryExecutor.execute(lastQuery);
            vsr = flightStream.getRoot();
            flightStream.next();
            long to = ((TimeStampMilliVector) vsr.getVector(timeCol)).get(0);
            setSamplingInterval(Duration.of(second - from, ChronoUnit.MILLIS));
            setTimeRange(new TimeRange(from, to));
        }
       catch (Exception e) {
            e.printStackTrace();
        }

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

    public String getConfig() {
        return config;
    }
}
