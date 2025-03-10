package gr.imsi.athenarc.visual.middleware.datasource.dataset;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDBDataset extends AbstractDataset {

    private static String DEFAULT_INFLUX_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBDataset.class);


    public InfluxDBDataset(){}
    
    // Abstract class implementation
    public InfluxDBDataset(String id, String bucket, String measurement){
        super(id, bucket, measurement, DEFAULT_INFLUX_FORMAT);
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

    @Override
    public String toString() {
        return "InfluxDBDataset{" +
                ", bucket='" + getSchema() + '\'' +
                ", measurement='" + getTableName() + '\'' +
                '}';
    }
}
