package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.datasource.QueryExecutor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.Dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.Query.Query;
import gr.imsi.athenarc.visual.middleware.domain.QueryResults;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MinMaxCache {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private final CacheQueryExecutor cacheQueryExecutor;
    private final CacheManager cacheManager;
    private final PrefetchManager prefetchManager;
    private final DataProcessor dataProcessor;

    public MinMaxCache(QueryExecutor dataQueryExecutor, AbstractDataset dataset, double prefetchingFactor, int aggFactor, int dataReductionRatio) {
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

