package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.cache.query.Query;
import gr.imsi.athenarc.visual.middleware.cache.query.QueryResults;
import gr.imsi.athenarc.visual.middleware.datasource.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MinMaxCache {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private final CacheQueryExecutor cacheQueryExecutor;
    private final CacheManager cacheManager;
    private final PrefetchManager prefetchManager;
    private final DataProcessor dataProcessor;

    protected MinMaxCache(QueryExecutor dataQueryExecutor, AbstractDataset dataset, double prefetchingFactor, int aggFactor, int dataReductionRatio) {
        // Constructor logic for MinMaxCache
        cacheQueryExecutor = new CacheQueryExecutor(dataset, aggFactor);
        cacheManager = new CacheManager(dataset.getMeasures());
        dataProcessor = new DataProcessor(dataQueryExecutor, dataset, dataReductionRatio);
        prefetchManager = new PrefetchManager(dataset, prefetchingFactor, cacheManager, dataProcessor);
    }

    public QueryResults executeQuery(Query query) {
        return cacheQueryExecutor.executeQuery(query, cacheManager, dataProcessor, prefetchManager);
    }

    public long calculateDeepMemorySize() {
        return 0;
    }
}

