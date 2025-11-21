package com.meiya.whalex.db.util.param.impl.file;

import jodd.datetime.JDateTime;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.parquet.example.data.simple.NanoTime;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author 黄河森
 * @date 2023/5/24
 * @package com.meiya.whalex.db.util.param.impl.file
 * @project whalex-data-driver
 */
public class NanoTimeUtils {

    static final long NANOS_PER_HOUR;
    static final long NANOS_PER_MINUTE;
    static final long NANOS_PER_SECOND;
    static final long NANOS_PER_DAY;
    private static final ThreadLocal<Calendar> parquetGMTCalendar;
    private static final ThreadLocal<Calendar> parquetLocalCalendar;

    public NanoTimeUtils() {
    }

    private static Calendar getGMTCalendar() {
        if (parquetGMTCalendar.get() == null) {
            parquetGMTCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("GMT")));
        }

        return (Calendar)parquetGMTCalendar.get();
    }

    private static Calendar getLocalCalendar() {
        if (parquetLocalCalendar.get() == null) {
            parquetLocalCalendar.set(Calendar.getInstance());
        }

        return (Calendar)parquetLocalCalendar.get();
    }

    public static Calendar getCalendar(boolean skipConversion) {
        Calendar calendar = skipConversion ? getLocalCalendar() : getGMTCalendar();
        calendar.clear();
        return calendar;
    }

    public static NanoTime getNanoTime(Timestamp ts, boolean skipConversion) {
        Calendar calendar = getCalendar(skipConversion);
        calendar.setTimeInMillis(ts.toEpochMilli());
        int year = calendar.get(1);
        if (calendar.get(0) == 0) {
            year = 1 - year;
        }

        JDateTime jDateTime = new JDateTime(year, calendar.get(2) + 1, calendar.get(5));
        int days = jDateTime.getJulianDayNumber();
        long hour = (long)calendar.get(11);
        long minute = (long)calendar.get(12);
        long second = (long)calendar.get(13);
        long nanos = (long)ts.getNanos();
        long nanosOfDay = nanos + NANOS_PER_SECOND * second + NANOS_PER_MINUTE * minute + NANOS_PER_HOUR * hour;
        return new NanoTime(days, nanosOfDay);
    }

    public static Timestamp getTimestamp(NanoTime nt, boolean skipConversion) {
        int julianDay = nt.getJulianDay();
        long nanosOfDay = nt.getTimeOfDayNanos();
        julianDay = (int)((long)julianDay + nanosOfDay / NANOS_PER_DAY);
        long remainder = nanosOfDay % NANOS_PER_DAY;
        if (remainder < 0L) {
            remainder += NANOS_PER_DAY;
            --julianDay;
        }

        JDateTime jDateTime = new JDateTime((double)julianDay);
        Calendar calendar = getCalendar(skipConversion);
        calendar.set(1, jDateTime.getYear());
        calendar.set(2, jDateTime.getMonth() - 1);
        calendar.set(5, jDateTime.getDay());
        int hour = (int)(remainder / NANOS_PER_HOUR);
        remainder %= NANOS_PER_HOUR;
        int minutes = (int)(remainder / NANOS_PER_MINUTE);
        remainder %= NANOS_PER_MINUTE;
        int seconds = (int)(remainder / NANOS_PER_SECOND);
        long nanos = remainder % NANOS_PER_SECOND;
        calendar.set(11, hour);
        calendar.set(12, minutes);
        calendar.set(13, seconds);
        Timestamp ts = Timestamp.ofEpochMilli(calendar.getTimeInMillis(), (int)nanos);
        return ts;
    }

    static {
        NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1L);
        NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1L);
        NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1L);
        NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1L);
        parquetGMTCalendar = new ThreadLocal();
        parquetLocalCalendar = new ThreadLocal();
    }

}
