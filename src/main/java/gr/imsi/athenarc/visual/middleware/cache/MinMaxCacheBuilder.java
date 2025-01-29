package gr.imsi.athenarc.visual.middleware.cache;


import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

public class MinMaxCacheBuilder {

    private DatasourceConnector datasourceConnector;
    private String schema;
    private String id;
    private double prefetchingFactor = 0.5;
    private int aggFactor = 4;
    private int dataReductionRatio = 4;

    public MinMaxCacheBuilder setDatasourceConnector(DatasourceConnector datasourceConnector) {
        this.datasourceConnector = datasourceConnector;
        return this;
    }

    public MinMaxCacheBuilder setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public MinMaxCacheBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public MinMaxCacheBuilder setPrefetchingFactor(double factor) {
        this.prefetchingFactor = factor;
        return this;
    }

    public MinMaxCacheBuilder setAggFactor(int aggFactor) {
        this.aggFactor = aggFactor;
        return this;
    }

    public MinMaxCacheBuilder setDataReductionRatio(int ratio) {
        this.dataReductionRatio = ratio;
        return this;
    }

    public MinMaxCache build() {
        if (datasourceConnector == null) {
            throw new IllegalStateException("Datasource connector must be provided");
        }
        AbstractDataset dataset = datasourceConnector.initializeDataset(schema, id);
        QueryExecutor queryExecutor = datasourceConnector.initializeQueryExecutor(dataset);
        return new MinMaxCache(queryExecutor, dataset, prefetchingFactor, aggFactor, dataReductionRatio);
    }
}