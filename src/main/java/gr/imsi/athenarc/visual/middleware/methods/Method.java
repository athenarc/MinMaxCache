package gr.imsi.athenarc.visual.middleware.methods;

import java.util.Map;

import gr.imsi.athenarc.visual.middleware.datasource.connector.DatasourceConnector;

/**
 * Common interface for all query methods.
 */
public interface Method {
    /**
     * Initialize the method once. This can be where you build caches or set up
     * data structures, using the parameters given.
     *
     * @param schema             Database schema or measurement name
     * @param datasetId          Unique dataset/table identifier
     * @param influxDBConnector  Connector to InfluxDB
     * @param params             Extra parameters for the method (accuracy, etc.)
     */
    void initialize(String schema, String datasetId, DatasourceConnector datasourceConnector, Map<String, String> params);

    /**
     * Execute a query using this method.
     *
     * @param query Query object with from/to, measures, filter, etc.
     * @return QueryResults
     */
    VisualQueryResults executeQuery(VisualQuery query);
}
