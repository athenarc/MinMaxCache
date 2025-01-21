package gr.imsi.athenarc.visual.middleware.datasource.initializer;

import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;

public interface DatasourceInitializer {
    AbstractDataset initializeDataset(String schema, String id);
    QueryExecutor initializeQueryExecutor(AbstractDataset dataset);
}