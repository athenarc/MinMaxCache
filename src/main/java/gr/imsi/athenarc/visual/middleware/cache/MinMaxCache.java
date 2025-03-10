package gr.imsi.athenarc.visual.middleware.cache;

import gr.imsi.athenarc.visual.middleware.cache.query.Query;
import gr.imsi.athenarc.visual.middleware.cache.query.QueryResults;
import gr.imsi.athenarc.visual.middleware.datasource.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MinMaxCache {
    private static final Logger LOG = LoggerFactory.getLogger(MinMaxCache.class);

    private final CacheQueryExecutor cacheQueryExecutor;
    private final CacheManager cacheManager;
    private final PrefetchManager prefetchManager;
    private final DataProcessor dataProcessor;

    public MinMaxCache(DataSource dataSource, double prefetchingFactor, int aggFactor, int dataReductionRatio) {
        cacheQueryExecutor = new CacheQueryExecutor(dataSource, aggFactor);
        cacheManager = new CacheManager(dataSource);
        dataProcessor = new DataProcessor(dataSource, dataReductionRatio);
        prefetchManager = new PrefetchManager(dataSource, prefetchingFactor, cacheManager, dataProcessor);
    }

    public QueryResults executeQuery(Query query) {
        return cacheQueryExecutor.executeQuery(query, cacheManager, dataProcessor, prefetchManager);
    }

    public long calculateDeepMemorySize() {
        return 0;
    }
}

