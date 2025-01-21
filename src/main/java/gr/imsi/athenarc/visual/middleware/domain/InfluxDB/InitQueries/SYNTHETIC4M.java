package gr.imsi.athenarc.visual.middleware.domain.influxdb.InitQueries;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.opencsv.bean.AbstractBeanField;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import gr.imsi.athenarc.visual.middleware.experiments.util.QueryUtils;

import java.text.ParseException;
import java.time.Instant;

@Measurement(name = "synthetic4m")
public class SYNTHETIC4M {

    @Column(timestamp = true)
    @CsvCustomBindByName(column="date", converter = EpochConverter.class)
    private Instant date;

    @Column
    @CsvBindByName(column = "value_1")
    private Double value_1;

    @Column
    @CsvBindByName(column = "value_2")
    private Double value_2;

    @Column
    @CsvBindByName(column = "value_3")
    private Double value_3;

    @Column
    @CsvBindByName(column = "value_4")
    private Double value_4;

    @Column
    @CsvBindByName(column = "value_5")
    private Double value_5;

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