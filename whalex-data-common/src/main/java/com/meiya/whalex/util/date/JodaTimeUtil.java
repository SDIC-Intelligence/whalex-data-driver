/**
 * Copyright©2013 Xiamen Meiah Pico IT CO., Ltd. 
 * All rights reserved.
 */
package com.meiya.whalex.util.date;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.*;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

/**
 * 引入joda-time来替换jdk原生的Date和Calendar<br/>
 * (日期的处理是系统中常用的功能。原生的Date由于设计问题使得在java中
 * 使用时间成为一项痛苦而繁杂的工作。
 * Calendar在一定程度上解决了部分Date的问题，但是却没有提供简单的方法，
 * 同时在Calendar上获取"正常"的日期也变得困难，
 * 而且Calendar上的多日历系统都是通过它的子类来实现的，显得非常笨重)
 * (joda-time是业界广泛接受的第三方开源时间实现方案，提供了更系统而直观的时间操作和使用接口，
 * (springFramework中也集成了对joda-time的支持)。
 * joda-time的作者也成为JDK8中新的时间实现的领导者)
 *
 * （joda-time简单易用的接口实际上已经基本不需要这个Util了，
 * 实现这个Util主要是列出一些joda-time的使用方式和作为DateUtil的延续）
 *
 * @author weic
 * @version 1.0 13-4-26 下午3:49
 */
public class JodaTimeUtil {

    public final static String DEFAULT_YMDHMS_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public final static String DEFAULT_YMD_FORMAT = "yyyy-MM-dd";
    public final static String SOLR_TDATE_FORMATE = "yyyy-MM-dd'T'HH:mm:ss.000'Z'";
    public final static String SOLR_TDATE_RETURN_FORMATE = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public final static String COMPACT_YMDHMS_FORMAT = "yyyyMMddHHmmss";
    public final static String COMPACT_Y_FORMAT = "yyyy";
    public final static String COMPACT_YM_FORMAT = "yyyyMM";
    public final static String COMPACT_YMD_FORMAT = "yyyyMMdd";
    public final static String COMPACT_YMDH_FORMAT = "yyyyMMddHH";
    private final static DateTime EARLY_DT;

    public final static Map<String, DateTimeFormatter> DATE_TIME_FORMATTER_MAP = new HashMap<String, DateTimeFormatter>();

    static {
        DATE_TIME_FORMATTER_MAP.put(DEFAULT_YMDHMS_FORMAT, DateTimeFormat.forPattern(DEFAULT_YMDHMS_FORMAT));
        DATE_TIME_FORMATTER_MAP.put(DEFAULT_YMD_FORMAT, DateTimeFormat.forPattern(DEFAULT_YMD_FORMAT));
        DATE_TIME_FORMATTER_MAP.put(SOLR_TDATE_FORMATE, DateTimeFormat.forPattern(SOLR_TDATE_FORMATE));
        DATE_TIME_FORMATTER_MAP.put(SOLR_TDATE_RETURN_FORMATE, DateTimeFormat.forPattern(SOLR_TDATE_RETURN_FORMATE));
        EARLY_DT = JodaTimeUtil.time("1990-01-01 08:00:00");
    }

    /**
     * 获得当前时间的joda-time对象
     * @return
     */
    public static DateTime currentTime(){
        //这里网上推荐用SystemFacotry.getClock().getDateTime(), 但是SystemFacotry不是jdk的原生类，所以暂时不用
        return new DateTime();
    }

    /**
     * 将时间字符串直接转换为joda-time对象
     * @param timeStr
     * @return
     */
    public static DateTime time(String timeStr){
        return time(timeStr, null);
    }

    /**
     * 将时间转换为归一化的类型，即转换为绝对秒数的字符类型
     * @param timeStr
     * @return
     */
    public static String time2Str(String timeStr) {
        DateTime time = time(timeStr);
        if (time == null) {
            return "";
        }
        return String.valueOf(time.getMillis() / 1000);
    }

    /**
     * 将时间转换为指定格式的字符串
     * @param timeMillis
     * @return
     */
    public static String time2Str(Long timeMillis) {
        if(null == timeMillis){
            return null;
        }
        DateTime dateTime = new DateTime(new Date(timeMillis));
        return dateTime.toString(DEFAULT_YMDHMS_FORMAT);
    }

    /**
     * 将时间转换为指定格式的字符串
     * @param timeMillis
     * @return
     */
    public static String time2Str(Long timeMillis, String pattern) {
        if(null == timeMillis){
            return null;
        }
        DateTime dateTime = new DateTime(new Date(timeMillis));
        return dateTime.toString(pattern);
    }

    /**
     * 将时间转换为指定格式的字符串
     * @param date
     * @param pattern
     * @return
     */
    public static String time2Str(Date date, String pattern) {
        DateTime dateTime = new DateTime(date);
        return dateTime.toString(pattern);
    }

    /**
     * 获得静态的formatter
     * @param pattern
     * @return
     */
    private static DateTimeFormatter getFormatter(String pattern){
        if(DATE_TIME_FORMATTER_MAP.containsKey(pattern)){
            return DATE_TIME_FORMATTER_MAP.get(pattern);
        } else {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
            DATE_TIME_FORMATTER_MAP.put(pattern, formatter);
            return formatter;
        }
    }

    /**
     * 将时间字符串直接转换为joda-time对象
     * @param timeStr
     * @param timePattern 时间格式
     * @return
     */
    public static DateTime time(String timeStr, String timePattern){
        if(StringUtils.isBlank(timeStr)){
            return null;
        }
        timeStr = StringUtils.trim(timeStr);
        if(StringUtils.isNotBlank(timePattern)){
            //有指定转换器
            return DateTimeFormat.forPattern(timePattern).parseDateTime(timeStr);
        } else {
            //没指定转换器
            if(timeStr.length() == 8){
                return getFormatter(COMPACT_YMD_FORMAT).parseDateTime(timeStr);
            } else if(timeStr.length() == 19){
                return getFormatter(DEFAULT_YMDHMS_FORMAT).parseDateTime(timeStr);
            } else if(timeStr.length() == 10){
                if(StringUtils.containsIgnoreCase(timeStr, "-")) {
                    return getFormatter(DEFAULT_YMD_FORMAT).parseDateTime(timeStr);
                } else {
                    return getFormatter(COMPACT_YMDH_FORMAT).parseDateTime(timeStr);
                }
            } else if(timeStr.length() == 14){
                return getFormatter(COMPACT_YMDHMS_FORMAT).parseDateTime(timeStr);
            } else if(timeStr.length() == 24){
                return getFormatter(SOLR_TDATE_FORMATE).parseDateTime(timeStr);
            } else if(timeStr.length() == 20){
                return getFormatter(SOLR_TDATE_RETURN_FORMATE).parseDateTime(timeStr);
            } else {
                return new DateTime(timeStr);
            }
        }
    }

    /**
     * 获得当前格式化以后的时间（yyyy-MM-dd HH:mm:ss）
     * @return
     */
    public static String now(){
        return now(DEFAULT_YMDHMS_FORMAT);
    }

    /**
     * 获得当前格式化以后的时间
     * @param pattern
     * @return
     */
    public static String now(String pattern){
        return currentTime().toString(pattern);
    }

    /**
     * 获得两个时间点之间的时间跨度
     * @param time1 开始的时间点
     * @param time2 结束的时间点
     * @param timeUnit 跨度的时间单位 see {@link JodaTime} （支持的时间单位有DAY,HOUR,MINUTE,SECOND,MILLI）
     */
    public static long lengthBetween(DateTime time1, DateTime time2, DurationFieldType timeUnit){
        if(timeUnit == JodaTime.DAY){
            return Days.daysBetween(time1, time2).getDays();
        } else if(timeUnit == JodaTime.WEEK){
            return Weeks.weeksBetween(time1, time2).getWeeks();
        } else if(timeUnit == JodaTime.HOUR){
            return Hours.hoursBetween(time1, time2).getHours();
        } else if(timeUnit == JodaTime.MINUTE){
            return Minutes.minutesBetween(time1, time2).getMinutes();
        } else if(timeUnit == JodaTime.SECOND){
            return Seconds.secondsBetween(time1, time2).getSeconds();
        } else if(timeUnit == JodaTime.MILLI){
            return Seconds.secondsBetween(time1, time2).getSeconds() * 1000L;
        } else if(timeUnit == JodaTime.YEAR){
            return Years.yearsBetween(time1, time2).getYears();
        } else {
            throw new RuntimeException("TimeUnit not supported except DAY,HOUR,MINUTE,SECOND,MILLI,WEEKS,YEAR");
        }
    }

    /**
     * 还原回jdk的原生Date对象
     * @return
     */
    public static Date parseDate(DateTime dateTime){
        return dateTime.toDate();
    }

    /**
     * 还原回jdk的原生Calendar对象
     * @return
     */
    public static Calendar parseCalendar(DateTime dateTime){
        return dateTime.toCalendar(Locale.getDefault());
    }

    /**
     * 将日期转化为字符串
     * @param date
     * @return
     */
    public static String timeStr(Date date){
        if(null == date){
            return "";
        }
        return new DateTime(date.getTime()).toString(DEFAULT_YMDHMS_FORMAT);
    }
    
    /**
     * 将日期转化为字符串
     * @param date
     * @return
     */
    public static String timeStrByLimtDate(Date date){
        if(null == date){
            return "";
        }
        if(new DateTime(date).compareTo(EARLY_DT) < 0){
            return "";
        }
        return new DateTime(date.getTime()).toString(DEFAULT_YMDHMS_FORMAT);
    }

    /**
     * 根据日期获取当天的开始时间字符串
     * @param date
     * @return
     */
    public static String startTimeStr(Date date){
        if(null == date){
            return "";
        }
        String dateStr = new DateTime(date.getTime()).toString(DEFAULT_YMD_FORMAT) + " 00:00:00";
        return dateStr;
    }

    /**
     * 根据日期获取当天的开始时间
     * @param date
     * @return
     */
    public static Date startTime(Date date){
        if(null == date){
            return null;
        }
        String dateStr = new DateTime(date.getTime()).toString(DEFAULT_YMD_FORMAT) + " 00:00:00";
        return DateTimeFormat.forPattern(DEFAULT_YMDHMS_FORMAT).parseDateTime(dateStr).toDate();
    }

    /**
     * 根据日期获取当天的结束时间
     * @param date
     * @return
     */
    public static Date endTime(Date date){
        if(null == date){
            return null;
        }
        String dateStr = new DateTime(date.getTime()).toString(DEFAULT_YMD_FORMAT) + " 23:59:59";
        return DateTimeFormat.forPattern(DEFAULT_YMDHMS_FORMAT).parseDateTime(dateStr).toDate();
    }

    /**
     * 根据日期获取当天的结束时间字符串
     * @param date
     * @return
     */
    public static String endTimeStr(Date date){
        if(null == date){
            return "";
        }
        String dateStr = new DateTime(date.getTime()).toString(DEFAULT_YMD_FORMAT) + " 23:59:59";
        return dateStr;
    }

    /**
     * 某个日期的几天前或者几天后的当天开始时间
     *
     * @param date
     *            给定的日期
     * @param dayOffset
     *            日期偏移量1标示一天后,-1标示1天前
     * @return
     */
    public static long startTimeWithDayOffset(Date date, int dayOffset) {
        if(date == null) date = new Date();
        long time = date.getTime() + Long.valueOf(dayOffset) * (1000 * 60 * 60 * 24);
        return startTime(new Date(time)).getTime();
    }

    public static Date startTimeWithDayOffsetDate(Date date, int dayOffset) {
        if(date == null) date = new Date();
        long time = date.getTime() + Long.valueOf(dayOffset) * (1000 * 60 * 60 * 24);
        return startTime(new Date(time));
    }

    /**
     * 某个日期的几天前或者几天后的当天结束时间
     *
     * @param date
     *            给定的日期
     * @param dayOffset
     *            日期偏移量1标示一天后,-1标示1天前
     * @return
     */
    public static long endTimeWithDayOffset(Date date, int dayOffset) {
        if(date == null) date = new Date();
        long time = date.getTime() + Long.valueOf(dayOffset) * (1000 * 60 * 60 * 24);
        return endTime(new Date(time)).getTime();
    }

    /**
     * 判断是否是同一天
     * @param date1
     * @param date2
     * @return
     */
    public static boolean sameDay(Date date1,Date date2){
        return date1.getYear() == date2.getYear() && date1.getMonth() == date2.getMonth() && date1.getDate() == date2.getDate();
    }

    /**
     * weic 判断数据时间是否肯定落在目标时间范围内，或者可能落在，或者绝不可能
     * @param startFilter 过滤开始时间
     * @param stopFilter 过滤结束时间
     * @param dataStart 数据开始时间
     * @param dataStop 数据结束时间
     * @return 1.肯定有， 0.可能有 -1.肯定没有
     */
    public static int timerangeCompare(Long startFilter, Long stopFilter, Long dataStart, Long dataStop){
        long EARLIST = JodaTimeUtil.time("1990-01-01 00:00:00").getMillis();
        if(startFilter < EARLIST){
            startFilter = null;
        }
        if(stopFilter < EARLIST){
            stopFilter = null;
        }
        if(dataStart < EARLIST){
            dataStart = null;
        }
        if(dataStop < EARLIST){
            dataStop = null;
        }

        if(null == dataStart && null == dataStop){
            //数据本身没有任何时间概念,可能存在
            return 0;
        } else if(null == startFilter && null == stopFilter){
            //没有过滤？可能存在
            return 0;
        } else if(null != startFilter && null != stopFilter){
            //两端时间都需要控制
            if(null != dataStart && null != dataStop){
                //数据同时有开始和结束时间
                if(dataStart >= startFilter && dataStop <= stopFilter){
                    return 1;
                } else if(dataStart > stopFilter || dataStop < startFilter){
                    return -1;
                } else {
                    return 0;
                }
            } else if(null == dataStart && null != dataStop){
                //数据只有结束时间
                if(dataStop < startFilter){
                    return -1;
                } else {
                    return 0;
                }
            } else if(null != dataStart && null == dataStop){
                //数据只有开始时间
                if(dataStart > stopFilter){
                    return -1;
                } else {
                    return 0;
                }
            }
        } else if(null != startFilter && null == stopFilter){
            //只控制开始时间
            if(null != dataStart && null != dataStop){
                //数据同时有开始和结束时间
                if(dataStart >= startFilter){
                    return 1;
                } else if(dataStop < startFilter){
                    return -1;
                } else {
                    return 0;
                }
            } else if(null == dataStart && null != dataStop){
                //数据只有结束时间
                if(dataStop < startFilter){
                    return -1;
                } else {
                    return 0;
                }
            } else if(null != dataStart && null == dataStop){
                //数据只有开始时间
                if(dataStart >= startFilter){
                    return 1;
                } else {
                    return 0;
                }
            }
        } else if(null == startFilter && null != stopFilter){
            //只控制结束时间
            if(null != dataStart && null != dataStop){
                //数据同时有开始和结束时间
                if(dataStop <= stopFilter){
                    return 1;
                } else if(dataStart > stopFilter){
                    return -1;
                } else {
                    return 0;
                }
            } else if(null == dataStart && null != dataStop){
                //数据只有结束时间
                if(dataStop <= stopFilter){
                    return 1;
                } else {
                    return 0;
                }
            } else if(null != dataStart && null == dataStop){
                //数据只有开始时间
                if(dataStart > stopFilter){
                    return -1;
                } else {
                    return 0;
                }
            }
        }

        return -1;
    }

    /**
     * 获取指定范围内地所有时间
     * @param date
     *        不包含date
     * @param beforeDay
     * 			之间几天
     * @param pattern
     * 			返回的时间格式
     * @return
     */
    public static List<String> getDateRange(Date date, int beforeDay, String pattern) {
        return getDateRange(date,beforeDay,pattern,false);
    }
    
    /**
     * 获取指定范围内地所有时间
     * @param date
     *
     * @param beforeDay
     * 			之间几天
     * @param pattern
     * 			返回的时间格式
     * @param containDate
     *          是否包含date
     * @return
     */
    public static List<String> getDateRange(Date date, int beforeDay, String pattern,boolean containDate) {
    	List<String> dates = new ArrayList<>();
    	DateTime dateTime = new DateTime(new Date());
    	if(!containDate){
            int i = beforeDay;
            while (i-- > 0) {
                dateTime = dateTime.minusDays(1);
                dates.add(dateTime.toString(pattern));
            }
        }else{
            for(int i =0;i<beforeDay;i++){
                dates.add(dateTime.minusDays(i).toString(pattern));
            }
        }
		return dates;
    }
    /**
      * @description 判断两个时间段是否有交集
      * @author liufy
      * @created 2019/4/12 13:11
      * @param t1 第一开始时间
      * @param t1e 第一个结束时间
      * @param t2 第二开始时间
      * @param t2e 第二个结束时间
      * @return
     **/
    public static boolean hasIntersection(long t1,long t1e,long t2,long t2e)
    {
        boolean inRang;
        if(t1>t2)
            inRang= t1>=t2 && t1<=t2e;
            //t1小,算出t2是否在t1~t1e
        else
            inRang= t2>=t1 && t2<=t1e;
        return inRang;
    }

}
