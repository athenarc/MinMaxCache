package gr.imsi.athenarc.visual.middleware.experiments.util;


import gr.imsi.athenarc.visual.middleware.util.DateTimeUtil;

import java.text.ParseException;


public class QueryUtils {

    public static Long convertToEpoch(String s) throws ParseException {
        return DateTimeUtil.parseDateTimeString(s);
    }

    public static Long convertToEpoch(String s, String timeFormat) throws ParseException {
        return DateTimeUtil.parseDateTimeString(s, timeFormat);
    }

}