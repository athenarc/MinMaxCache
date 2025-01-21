package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;

public class MinMaxCacheBuilder {
    private DatasourceConnector initializer;
    private String schema;
    private String id;
    private double prefetchingFactor = 0.5;
    private int aggFactor = 4;
    private int dataReductionRatio = 4;

    public MinMaxCacheBuilder setCacheInitializer(DatasourceConnector initializer) {
        this.initializer = initializer;
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
        if (initializer == null) {
            throw new IllegalStateException("CacheInitializer must be provided");
        }
        AbstractDataset dataset = initializer.initializeDataset(schema, id);
        QueryExecutor queryExecutor = initializer.initializeQueryExecutor(dataset);
        return new MinMaxCache(queryExecutor, dataset, prefetchingFactor, aggFactor, dataReductionRatio);
    }
}