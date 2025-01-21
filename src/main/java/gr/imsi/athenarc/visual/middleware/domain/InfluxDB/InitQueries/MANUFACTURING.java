package gr.imsi.athenarc.visual.middleware.domain.influxdb.InitQueries;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.opencsv.bean.AbstractBeanField;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import gr.imsi.athenarc.visual.middleware.experiments.util.QueryUtils;

import java.text.ParseException;
import java.time.Instant;

@Measurement(name = "manufacturing")
public class MANUFACTURING {

    @Column(timestamp = true)
    @CsvCustomBindByName(column="0", converter = EpochConverter.class)
    private Instant datetime;

    @Column
    @CsvBindByName(column = "1")
    private Double value_1;

    @Column
    @CsvBindByName(column = "2")
    private Double value_2;

    @Column
    @CsvBindByName(column = "3")
    private Double value_3;

    @Column
    @CsvBindByName(column = "4")
    private Double value_4;

    @Column
    @CsvBindByName(column = "5")
    private Double value_5;

    @Column
    @CsvBindByName(column = "6")
    private Double value_6;

    @Column
    @CsvBindByName(column = "7")
    private Double value_7;

    public static class EpochConverter extends AbstractBeanField {

        @Override
        public Instant convert(String s) {
            try {
                return Instant.ofEpochMilli(QueryUtils.convertToEpoch(s, "yyyy-MM-dd[ HH:mm:ss.SSS]"));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}