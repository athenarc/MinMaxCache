package gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor;

import cfjd.com.fasterxml.jackson.annotation.JsonIgnore;
import gr.imsi.athenarc.visual.middleware.datasource.DataSourceQuery;
import gr.imsi.athenarc.visual.middleware.datasource.ModelarDBQuery;
import gr.imsi.athenarc.visual.middleware.domain.DataPoint;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.Query.QueryMethod;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;
import gr.imsi.athenarc.visual.middleware.domain.TableInfo;
import gr.imsi.athenarc.visual.middleware.experiments.util.PrepareSQLStatement;
import cfjd.org.apache.arrow.flight.FlightClient;
import cfjd.org.apache.arrow.flight.FlightStream;
import cfjd.org.apache.arrow.flight.Ticket;
import cfjd.org.apache.arrow.vector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public class ModelarDBQueryExecutor implements QueryExecutor, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ModelarDBQueryExecutor.class);
    @JsonIgnore
    FlightClient flightClient;
    AbstractDataset dataset;

    public ModelarDBQueryExecutor(FlightClient flightClient, AbstractDataset dataset) {
        this.flightClient = flightClient;
        this.dataset = dataset;
        LOG.info("Created Executor {}, ", this);
    }


    public ModelarDBQueryExecutor(FlightClient flightClient) {
        this.flightClient = flightClient;
        LOG.info("Created Executor {}, ", this);
    }

    @Override
    public QueryResults execute(DataSourceQuery q, QueryMethod method) throws SQLException {
        switch (method) {
            case M4:
                return executeM4Query(q);
            case RAW:
                return executeRawQuery(q);
            case MIN_MAX:
                return executeMinMaxQuery(q);
            default:
                throw new UnsupportedOperationException("Unsupported Query Method");
        }
    }

    @Override
    public QueryResults executeM4Query(DataSourceQuery q) throws SQLException {
        return collect(executeM4ModelarDBQuery((ModelarDBQuery) q));
    }


    @Override
    public QueryResults executeRawQuery(DataSourceQuery q) throws SQLException {
        return collect(executeRawModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public QueryResults executeMinMaxQuery(DataSourceQuery q) throws SQLException {
        return collect(executeMinMaxModelarDBQuery((ModelarDBQuery) q));
    }

    @Override
    public void initialize(String path) throws SQLException {
    }

    @Override
    public void drop() throws SQLException {

    }

    @Override
    public List<TableInfo> getTableInfo() throws SQLException {
        return null;
    }

    @Override
    public List<String> getColumns(String tableName) throws SQLException {
        return null;
    }

    @Override
    public List<Object[]> getSample(String schema, String tableName) throws SQLException {
        return null;
    }

    Comparator<DataPoint> compareLists = new Comparator<DataPoint>() {
        @Override
        public int compare(DataPoint s1, DataPoint s2) {
            if (s1==null && s2==null) return 0; //swapping has no point here
            if (s1==null) return  1;
            if (s2==null) return -1;
            return (int) (s1.getTimestamp() - s2.getTimestamp());
        }
    };


    public FlightStream executeRawModelarDBQuery(ModelarDBQuery q) throws SQLException{
        String sql = q.rawQuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();

        return execute(query);
    }
    public FlightStream executeM4ModelarDBQuery(ModelarDBQuery q) throws SQLException {
        String sql = q.m4QuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();

        return execute(query);
    }


    public FlightStream executeMinMaxModelarDBQuery(ModelarDBQuery q) throws SQLException {
        LOG.debug("Executing {} with {}, ", q, dataset);

        String sql = q.minMaxQuerySkeleton();
        PrepareSQLStatement preparedStatement = new PrepareSQLStatement(sql);
        preparedStatement.setLong("from", q.getFrom());
        preparedStatement.setLong("to", q.getTo());
        preparedStatement.setString("timeCol", dataset.getTimeCol());
        preparedStatement.setString("valueCol", dataset.getValueCol());
        preparedStatement.setString("idCol", dataset.getIdCol());
        preparedStatement.setString("tableName", dataset.getTable());
        String query = preparedStatement.getSql();
        return execute(query);
    }

    private QueryResults collect(FlightStream flightStream)  {
        QueryResults queryResults = new QueryResults();
        HashMap<Integer, List<DataPoint>> data = new HashMap<>();
        while (flightStream.next()) {
            VectorSchemaRoot vsr = flightStream.getRoot();
            int rowCount = vsr.getRowCount();
        }
        try {
            flightStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        queryResults.setData(data);
        return queryResults;
    }

    public FlightStream execute(String query) throws SQLException {
        LOG.info("Executing Query: \n" + query);
        Ticket ticket = new Ticket(query.getBytes());
        return flightClient.getStream(ticket);
    }

    public AbstractDataset getDataset() {
        return dataset;
    }

    public Comparator<DataPoint> getCompareLists() {
        return compareLists;
    }

    public void setDataset(AbstractDataset dataset) {
        this.dataset = dataset;
    }

    @Override
    public String toString() {
        return "ModelarDBQueryExecutor{" +
                ", dataset=" + dataset +
                ", compareLists=" + compareLists +
                '}';
    }
}
