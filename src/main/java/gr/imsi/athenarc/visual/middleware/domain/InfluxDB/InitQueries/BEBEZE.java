package gr.imsi.athenarc.visual.middleware.domain.influxdb.InitQueries;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.opencsv.bean.AbstractBeanField;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvCustomBindByName;
import gr.imsi.athenarc.visual.middleware.experiments.util.QueryUtils;

import java.text.ParseException;
import java.time.Instant;

@Measurement(name = "bebeze")
public class BEBEZE {

    @Column(timestamp = true)
    @CsvCustomBindByName(column="datetime", converter = EpochConverter.class)
    private Instant datetime;

    @Column
    @CsvBindByName(column = "active_power")
    private Double active_power;

    @Column
    @CsvBindByName(column = "roto_speed")
    private Double roto_speed;

    @Column
    @CsvBindByName(column = "wind_speed")
    private Double wind_speed;

    @Column
    @CsvBindByName(column = "cos_nacelle_dir")
    private Double cos_nacelle_dir;

    @Column
    @CsvBindByName(column = "pitch_angle")
    private Double pitch_angle;

    @Column
    @CsvBindByName(column = "sin_nacelle_dir")
    private Double sin_nacelle_dir;

    @Column
    @CsvBindByName(column = "cos_wind_dir")
    private Double cos_wind_dir;

    @Column
    @CsvBindByName(column = "sin_wind_dir")
    private Double sin_wind_dir;

    @Column
    @CsvBindByName(column = "nacelle_direction")
    private Double nacelle_direction;

    @Column
    @CsvBindByName(column = "wind_direction")
    private Double wind_direction;

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