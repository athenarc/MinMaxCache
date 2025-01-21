package gr.imsi.athenarc.visual.middleware.datasource.connector;

import java.io.IOException;

import gr.imsi.athenarc.visual.middleware.datasource.executor.CsvQueryExecutor;
import gr.imsi.athenarc.visual.middleware.datasource.executor.QueryExecutor;
import gr.imsi.athenarc.visual.middleware.domain.csv.CsvConfiguration;
import gr.imsi.athenarc.visual.middleware.domain.dataset.AbstractDataset;
import gr.imsi.athenarc.visual.middleware.domain.dataset.CsvDataset;

public class CsvConnector implements DatasourceConnector {

    private final CsvConfiguration csvConfiguration;

    public CsvConnector(CsvConfiguration csvConfiguration) {
        this.csvConfiguration = csvConfiguration;
    }


    @Override
    public AbstractDataset initializeDataset(String schema, String id) {
        try {
            return new CsvDataset(csvConfiguration.getPath(), id, schema, id, csvConfiguration.getTimeFormat(), csvConfiguration.getTimeCol(), csvConfiguration.getDelimiter(), csvConfiguration.getHasHeader());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public QueryExecutor initializeQueryExecutor(AbstractDataset dataset) {
        try {
            return new CsvQueryExecutor(dataset);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    
}
