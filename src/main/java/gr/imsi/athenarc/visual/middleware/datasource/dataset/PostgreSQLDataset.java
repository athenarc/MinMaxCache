package gr.imsi.athenarc.visual.middleware.datasource.dataset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLDataset extends AbstractDataset {

    private static String DEFAULT_POSTGRES_FORMAT = "yyyy-MM-dd[ HH:mm:ss.SSS]";

    private String timeCol;
    private String idCol;
    private String valueCol;

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLDataset.class);

    public PostgreSQLDataset(){}
    
    // Abstract class implementation
    public PostgreSQLDataset(String id, String schema, String table){
        super(id, schema, table, DEFAULT_POSTGRES_FORMAT);
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
    
    public String getTimeCol() {
        return timeCol;
    }

    public void setTimeCol(String timeCol) {
        this.timeCol = timeCol;
    }

    public String getIdCol() {
        return idCol;
    }

    public void setIdCol(String idCol) {
        this.idCol = idCol;
    }

    public String getValueCol() {
        return valueCol;
    }

    public void setValueCol(String valueCol) {
        this.valueCol = valueCol;
    }

}
