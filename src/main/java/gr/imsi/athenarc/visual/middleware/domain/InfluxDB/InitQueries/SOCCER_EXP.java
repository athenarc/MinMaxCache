package gr.imsi.athenarc.visual.middleware.domain.influxdb.InitQueries;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.opencsv.bean.AbstractBeanField;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import gr.imsi.athenarc.visual.middleware.experiments.util.QueryUtils;

import java.text.ParseException;
import java.time.Instant;

@Measurement(name = "soccer_exp")
public class SOCCER_EXP {

    @Column(timestamp = true)
    @CsvCustomBindByName(column="datetime", converter = EpochConverter.class)
    private Instant datetime;

    @Column
    @CsvBindByName(column = "x")
    private Double x;

    @Column
    @CsvBindByName(column = "y")
    private Double y;

    @Column
    @CsvBindByName(column = "z")
    private Double z;

    @Column
    @CsvBindByName(column = "abs_vel")
    private Double abs_vel;

    @Column
    @CsvBindByName(column = "abs_accel")
    private Double abs_accel;

    @Column
    @CsvBindByName(column = "vx")
    private Double vx;

    @Column
    @CsvBindByName(column = "vy")
    private Double vy;

    @Column
    @CsvBindByName(column = "vz")
    private Double vz;

    @Column
    @CsvBindByName(column = "ax")
    private Double ax;

    @Column
    @CsvBindByName(column = "ay")
    private Double ay;

    @Column
    @CsvBindByName(column = "az")
    private Double az;

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