package gr.imsi.athenarc.visual.middleware.domain.influxdb.InitQueries;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.opencsv.bean.AbstractBeanField;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import gr.imsi.athenarc.visual.middleware.experiments.util.QueryUtils;

import java.text.ParseException;
import java.time.Instant;

@Measurement(name = "intel_lab_exp")
public class INTEL_LAB_EXP {

    @Column(timestamp = true)
    @CsvCustomBindByName(column="datetime", converter = EpochConverter.class)
    private Instant datetime;

    @Column
    @CsvBindByName(column = "moteid")
    private Double moteid;

    @Column
    @CsvBindByName(column = "temperature")
    private Double temperature;

    @Column
    @CsvBindByName(column = "humidity")
    private Double humidity;

    @Column
    @CsvBindByName(column = "light")
    private Double light;

    @Column
    @CsvBindByName(column = "voltage")
    private Double voltage;

    public static class EpochConverter extends AbstractBeanField {

        @Override
        public Instant convert(String s) {
            try {
                return Instant.ofEpochMilli(QueryUtils.convertToEpoch(s, "yyyy-MM-dd[ HH:mm:ss]"));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}