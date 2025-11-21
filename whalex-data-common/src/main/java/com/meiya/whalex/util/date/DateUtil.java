package com.meiya.whalex.util.date;

import cn.hutool.core.date.DatePattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Date Utility Class This is used to convert Strings to Dates and Timestamps
 * 
 * <p>
 * <a href="DateUtil.java.html"><i>View Source</i></a>
 * </p>
 * 
 * @author <a href="mailto:matt@raibledesigns.com">Matt Raible</a> Modified by
 *         <a href="mailto:dan@getrolling.com">Dan Kibler </a> to correct time
 *         pattern. Minutes should be mm not MM (MM is month).
 */
@Slf4j
public class DateUtil {

	private static String timePattern = "HH:mm";

	private static Random random = new Random(System.currentTimeMillis());

	public static final String yyyy_MM_dd = "yyyy-MM-dd";

	public static final String yyyyMMdd = "yyyyMMdd";

	public static final String yyyy_MM_dd_HH_mm_ss_S = "yyyy-MM-dd HH:mm:ss.S";

	public static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";

	public static final String yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";

	/**
	 * 可以匹配的时间格式
	 */
	public static List<String> dateFormatList;

	/**
	 * 时间格式数组串
	 */
	public static String[] dateFormatArray = null;
	static {


		dateFormatList = new ArrayList<>();

		dateFormatList.add("yyyy-MM-dd HH:mm:ss.S");
		dateFormatList.add("yyyy-MM-dd HH:mm:ss");
		dateFormatList.add("yyyy-MM-dd H:mm:ss.M");
		dateFormatList.add("yyyy-MM-dd H:mm:ss.Z");
		dateFormatList.add("yyyy-MM-dd H:mm:ss");
		dateFormatList.add("yyyy-MM-dd HH:mm");
		dateFormatList.add("yyyy-MM-dd H:mm");
		dateFormatList.add("yyyy-MM-dd");
		dateFormatList.add("yyyy-MM-ddHH:mm:ss.S");
		dateFormatList.add("yyyy-MM-ddHH:mm:ss");

		dateFormatList.add("yyyy/MM/dd HH:mm:ss");
		dateFormatList.add("yyyy/M/dd HH:mm:ss");
		dateFormatList.add("dd/MM/yyyy HH:mm:ss");

				/**
				 * 2018年12月13日15:34:28 kdg
				 * list 从上往下解析，@see org.apache.commons.lang.time.DateUtils.parseDateStrictly (饥饿返回 fast return）
				 * 从头开始匹配上就会返回，时间口径（精度）会越来越大
				 */
		dateFormatList.add("yyyyMMddHH:mm:ss");// eg :2017030118:40:02
		dateFormatList.add("yyyy/MM/dd HH:mm:ss.S");
		dateFormatList.add("yyyy年MM月dd日 HH:mm:ss.S");
		dateFormatList.add("yyyyMMdd HH:mm:ss.S");
		dateFormatList.add("dd-MM月 -yy hh.mm.ss.S 下午");
		dateFormatList.add("dd-MM月 -yy hh.mm.ss.S 上午");
		dateFormatList.add("dd-MM月-yy hh.mm.ss.S 上午");
		dateFormatList.add("dd-MM月-yy hh.mm.ss.S 下午");

		dateFormatList.add("dd-MM月 -yy hh.mm.ss.S 下午");
		dateFormatList.add("dd-MM月 -yy hh.mm.ss.S 上午");
		dateFormatList.add("dd-MM月-yy hh.mm.ss.S 上午");
		dateFormatList.add("dd-MM月-yy hh.mm.ss.S 下午");

		dateFormatList.add("yyyy/MM/dd HH:mm");
		dateFormatList.add("yyyy/MM/dd H:mm");
		dateFormatList.add("yyyy/M/dd HH:mm");

		dateFormatList.add("yyyyMMdd HH:mm:ss");
				//20170704 025959
		dateFormatList.add("yyyyMMdd HHmmss");

		dateFormatList.add("yyyyMMddHHmmssSSS");
		dateFormatList.add("yyyyMMddHHmmss");
		dateFormatList.add("yyyyMMddHHmm");

		dateFormatList.add("yyyy/MM/dd");
		dateFormatList.add("yyyy.MM.dd");
		dateFormatList.add("yyyy/M/dd");

		dateFormatList.add("yyyyMMddHH");
		dateFormatList.add("yyyyMMdd");
				/**
				 * 年月
				 * */
		dateFormatList.add("yyyyMM");
				/**
				 * 2016年05月12
				 * */
		dateFormatList.add("yyyy年MM月dd日");
		dateFormatList.add("yyyy年MM月dd");
		dateFormatList.add("yyyy年MM月");



		/**
		 * 转换成数组
		 */
		dateFormatArray = new String[dateFormatList.size()];
		dateFormatList.toArray(dateFormatArray);
	}

	// ~ Methods
	// ================================================================

	/**
	 * Return default datePattern (MM/dd/yyyy)
	 * 
	 * @return a string representing the date pattern on the UI
	 */
	public static String getDatePattern() {
		return yyyy_MM_dd;
	}

	public static String getDateTimePattern() {
		return yyyy_MM_dd_HH_mm_ss_S;
	}

	public static final String getNowYear() {
		return String.valueOf(DateTime.now().getYear());
	}

	/**
	 * This method attempts to convert an Oracle-formatted date in the form
	 * dd-MMM-yyyy to mm/dd/yyyy.
	 * 
	 * @param aDate
	 *            date from database as a string
	 * @return formatted string for the ui
	 */
	public static final String getDate(Date aDate) {
		DateTime dt = new DateTime(aDate.getTime());
		return dt.toString(yyyy_MM_dd);
	}

	public static final Date getDate(String aDate) {
		try {
			return convertStringToDate(aDate);
		} catch (ParseException e) {
		}
		return null;
	}

	/**
	 * This method generates a string representation of a date/time in the
	 * format you specify on input
	 * 
	 * @param aMask
	 *            the date pattern the string is in
	 * @param strDate
	 *            a string representation of a date
	 * @return a converted Date object
	 * @see SimpleDateFormat
	 * @throws ParseException
	 */
	public static final Date convertStringToDate(String aMask, String strDate) throws ParseException {
		DateTime dt = DateTime.parse(strDate, DateTimeFormat.forPattern(aMask));
		return dt.toDate();
	}

	/**
	 * This method returns the current date time in the format: MM/dd/yyyy HH:MM
	 * a
	 *
	 * @param theTime
	 *            the current time
	 * @return the current date/time
	 */
	public static String getTimeNow(Date theTime) {
		return getDateTime(timePattern, theTime);
	}

	/**
	 * This method returns the current date in the format: MM/dd/yyyy
	 *
	 * @return the current date
	 * @throws ParseException
	 */
	public static Calendar getToday() throws ParseException {
		DateTime dt = DateTime.now();
		return new DateTime(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), 0, 0, 0, 0).toCalendar(null);
	}

	/**
	 * This method generates a string representation of a date's date/time in
	 * the format you specify on input
	 *
	 * @param aMask
	 *            the date pattern the string is in
	 * @param aDate
	 *            a date object
	 * @return a formatted string representation of the date
	 *
	 * @see SimpleDateFormat
	 */
	public static final String getDateTime(String aMask, Date aDate) {
		if (aDate == null) {
			log.error("aDate is null!");
			return "";
		}
		DateTime dt = new DateTime(aDate.getTime());
		return dt.toString(aMask);

	}

	/**
	 * This method generates a string representation of a date based on the
	 * System Property 'dateFormat' in the format you specify on input
	 * 
	 * @param aDate
	 *            A date to convert
	 * @return a string representation of the date
	 */
	public static final String convertDateToString(Date aDate) {
		return getDateTime(getDatePattern(), aDate);
	}

	/**
	 * This method converts a String to a date using the datePattern
	 * 
	 * @param strDate
	 *            the date to convert (in format MM/dd/yyyy)
	 * @return a date object
	 * 
	 * @throws ParseException
	 */
	public static Date convertStringToDate(String strDate) throws ParseException {
		return convertStringToDate(getDatePattern(), strDate);
	}

	/**
	 * 格式化指定的时间，格式为 yyyy-MM-dd HH:mm:ss
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDateTime(Date date) {
		return formatDate(date, yyyy_MM_dd_HH_mm_ss);
	}

	/**
	 * 格式化日期，日期格式都为：yyyy-MM-dd HH:mm:ss
	 * 
	 * @param o
	 * @return
	 */
	public static String formatDate(Object o) {
		if (o == null)
			return "";
		if (o.getClass() == String.class) {
			DateTime dt = DateTime.parse((String) o, DateTimeFormat.forPattern(yyyy_MM_dd_HH_mm_ss));
			return dt.toString(yyyy_MM_dd_HH_mm_ss);
		} else if (o.getClass() == Date.class) {
			return formatDate((Date) o, yyyy_MM_dd_HH_mm_ss);
		} else if (o.getClass() == Timestamp.class) {
			return conversion(((Timestamp) o).getTime());
		} else
			return o.toString();
	}

	/**
	 * 给时间加上或减去指定毫秒，秒，分，时，天、月或年等，返回变动后的时间
	 *
	 * @param date
	 *            要加减前的时间，如果不传，则为当前日期
	 * @param field
	 *            时间域，有Calendar.MILLISECOND,Calendar.SECOND,Calendar.MINUTE,<br>
	 *            Calendar.HOUR,Calendar.DATE, Calendar.MONTH,Calendar.YEAR
	 * @param amount
	 *            按指定时间域加减的时间数量，正数为加，负数为减。
	 * @return 变动后的时间
	 */
	public static Date add(Date date, int field, int amount) {
		if (date == null) {
			date = new Date();
		}

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(field, amount);

		return cal.getTime();
	}

	public static Date addDay(Date date, int amount) {
		return add(date, Calendar.DATE, amount);
	}

	public static Date addSecond(Date date, int amount) {
		return add(date, Calendar.SECOND, amount);
	}

	public static Date addMonth(Date date, int amount) {
		return add(date, Calendar.MONTH, amount);
	}

	public static Date addYear(Date date, int amount) {
		return add(date, Calendar.YEAR, amount);
	}

	public static Date addHour(Date date, int amount) {
		return add(date, Calendar.HOUR, amount);
	}

	/**
	 * 根据指定格式，格式化日期，若Pattern为null，则默认为yyyy-MM-dd
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String formatDate(Date date, String pattern) {
		if (date == null)
			return "";
		if (pattern == null)
			pattern = "yyyy-MM-dd";
		DateTime dt = new DateTime(date.getTime());
		return dt.toString(pattern);
	}

	public static String formatString(String time, String pattern) {
		DateTime dt = DateTime.parse(time, DateTimeFormat.forPattern(yyyyMMddHHmmss));
		return dt.toString(pattern);
	}

	public final static String getIncrementNo() {
		int nextInt = random.nextInt(9999);
		StringBuffer stringBuffer = new StringBuffer(DateTime.now().toString(yyyyMMddHHmmss));
		String valueOf = String.valueOf(nextInt);
		if (valueOf.length() < 4) {
			for (int i = 0; i < 4 - valueOf.length(); i++) {
				stringBuffer.append(0);
			}
		}
		stringBuffer.append(valueOf);
		return stringBuffer.toString();
	}

	/**
	 * 转换字符串时间为日期格式
	 * 
	 * @param stime
	 * @return
	 * @author xujb@xm-my.com.cn
	 */
	public static Date parseDate(String stime) {
		try {
			return DateUtils
					.parseDate(StringUtils.trim(stime),
							new String[] { "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "yyyy/M/dd HH:mm:ss",
									"yyyyMMddHHmmss", "yyyy-MM-dd", "yyyy/M/dd", "yyyyMMdd", "yyyy/MM/dd",
									"yyyyMMdd HH:mm:ss", DatePattern.UTC_WITH_ZONE_OFFSET_PATTERN,
									DatePattern.UTC_MS_PATTERN, DatePattern.UTC_MS_WITH_ZONE_OFFSET_PATTERN});
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * 转换字符串时间为日期格式
	 * 
	 * @param stime
	 * @return
	 * @author xujb@xm-my.com.cn
	 */
	public static Date parseDate4Imp(String stime) {
		try {
			Date datet = DateUtils.parseDate(StringUtils.trim(stime),
					new String[] { "dd/MM/yyyy", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss",
							"yyyy/M/dd HH:mm:ss", "yyyyMMddHHmmss", "yyyy-MM-dd", "yyyy/M/dd", "yyyyMMdd",
							"yyyy/MM/dd" });
			return datet;
		} catch (Exception e) {
			return new Date();
		}
	}

	// 时间转化成UNIX时间戳，绝对秒数
	public static long conversion(String new_date_f) {
		try {
			if (StringUtils.isBlank(new_date_f)) {
				return 0;
			}
			
			
			new_date_f = StringUtils.trim(new_date_f);
			if ("0".equals(new_date_f) || "null".equals(new_date_f.toLowerCase())||"no".equals(new_date_f.toLowerCase())) {
				return 0;
			}

			/**
			 * 改进，以适应不同的时间格式过来的字符串
			 * 
			 * 改进，调整格式匹配串之间的顺序 "yyyy-MM-dd H:mm:ss.M", "yyyy-MM-dd H:mm:ss"
			 * "yyyyMMddHHmmss", "yyyyMMddHHmm"
			 */

			Date date = null;
			try {
				date = DateUtils.parseDateStrictly(new_date_f, dateFormatArray);
			} catch (ParseException e) {
				// 以非严格模式在解析一次
                try {
                    date = DateUtils.parseDate(new_date_f, dateFormatArray);
                } catch (ParseException e1) {
//                    e1.printStackTrace();
                    // maybe str has time zone character eg : 2017-12-25 17:46:25+08
                    try {
                        date = parserTimeZoneDate(new_date_f, "GMT+8");
                    } catch (ParseException e2) {
                        e2.printStackTrace();
						return 0;
                    }
                }
            }

			SimpleDateFormat sdf = new SimpleDateFormat(yyyy_MM_dd_HH_mm_ss);
			new_date_f = sdf.format(date);
			return DateTime.parse(new_date_f, DateTimeFormat.forPattern(yyyy_MM_dd_HH_mm_ss)).getMillis() / 1000;
		} catch (Exception e) {

            e.printStackTrace();

        }
		return 0;
	}



	// 毫秒转化成时间
	public static String conversion(long time) {
		return new DateTime(time).toString(yyyy_MM_dd_HH_mm_ss);
	}

	// 毫秒转化成时间
	public static String conversion(long time, String pattern) {
		return new DateTime(time).toString(pattern);
	}

	/**
	 * 将时间串转化成solr存储的时间格式，否则存入solr的时间将少8小时
	 * 
	 * @param time
	 * @return
	 */
	public static String convertToSolrDate(String time) {
		if (time != null && !"".equals(time)) {
			time = time.replace(" ", "T") + "Z";
		}
		return time;
	}

	public static String formatDateTime() {
		return (formatDate(now(), yyyy_MM_dd_HH_mm_ss));
	}

	public static String timeStampToString(Timestamp now) {
		return conversion(now.getTime());
	}

	public static Date now() {
		return DateTime.now().toDate();
	}

	public static String getNowDate() {
		return (formatDate(now(), "yyyyMMddHHmmssSSS"));
	}

	/**
	 * 
	 * @param startDate
	 *            开始时间,包括时分秒
	 * @param endDate
	 *            结束时间,包括时分秒
	 * @return 根据开始时间和结束时间构造一个时间集合,例如startDate='2012-11-12 08:03:10',
	 *         endDate='2012-11-14 09:05:08',则返回的集合如下所示{2012-11-12
	 *         08:03:10,2012-11-12 23:59:59} ,{2012-11-13 00:00:00,2012-11-13
	 *         23:59:59},{2012-11-14 00:00:00,2012-11-14 09:05:08} history: add
	 *         by delcan-刘仁杰
	 */
	public static List<String[]> getDateRange(final String startDate, final String endDate) {
		// 判断开始时间如果为空，则返回null
		if (StringUtils.isEmpty(startDate) || StringUtils.isBlank(endDate))
			return null;

		// 判断结束时间如果为空，则返回null
		if (StringUtils.isEmpty(endDate) || StringUtils.isBlank(endDate))
			return null;

		// 当开始日期大于结束日期,则视为无效区间,返回null
		if (compareDateTime(startDate, endDate))
			return null;

		// 定义日期数组
		List<String[]> dateRange = new ArrayList<String[]>();
		String[] startDates = startDate.split(" ");
		String[] endDates = endDate.split(" ");
		String tmpStartDate = startDates[0];
		String tmpEndDate = endDates[0];

		// 定义日期的显示格式
		try {
			while (true) {
				DateTime dt = DateTime.parse(tmpStartDate, DateTimeFormat.forPattern(yyyy_MM_dd));
				tmpEndDate = dt.plusDays(1).toString(yyyy_MM_dd);

				if (compareDateTime(tmpStartDate, endDates[0])) {
					if (startDates[0].equals(endDates[0])) {
						dateRange.add(new String[] { startDate, endDate });
					} else {
						dateRange.add(new String[] { tmpStartDate + " 00:00:00", tmpStartDate + " " + endDates[1] });
					}
					break;
				} else {
					dateRange.add(
							new String[] { tmpStartDate + " " + (dateRange.size() == 0 ? startDates[1] : "00:00:00"),
									tmpStartDate + " 23:59:59" });
					tmpStartDate = tmpEndDate;
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		// 返回日期集合
		return dateRange;
	}

	/**
	 * 
	 * @param startDate
	 *            开始日期,可以包含时分秒
	 * @param endDate
	 *            结束日期，可以包含时分秒
	 * @return
	 * @throws ParseException
	 * @throws NumberFormatException
	 */
	public static boolean compareDateTime(final String startDate, final String endDate) {
		Long start = Long.parseLong(startDate.replaceAll("\\D", ""));
		Long end = Long.parseLong(endDate.replaceAll("\\D", ""));
		return start >= end;
	}

	/**
	 * 获取几天前的时间
	 * 
	 * @param beforeDay
	 * @return
	 */
	public static Date getBeforeDay(int beforeDay) {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -beforeDay);
		return DateUtils.truncate(cal.getTime(), Calendar.DATE);
	}

	public static void main4(String[] args) {
		System.out.println(
				"from{2017-05-17} -->" + formatDate(new Date(conversion("2017-05-17") * 1000), yyyy_MM_dd_HH_mm_ss));
		System.out.println("from{17/05/2017 09:08:33} -->"
				+ formatDate(new Date(conversion("17/05/2017 09:08:33") * 1000), yyyy_MM_dd_HH_mm_ss));
	}
	
	
	/**
	 * catm、latm、fitm本身有时间但异常的，直接放空。时间在1995年以前，2050年以后的直接清空
	 * 
	 * @author daihb 20180327
	 */
	// 1995-01-01 00:00:00=788889600
	public final static String VALID_START_DATE = "1995-01-01 00:00:00";
	public final static long VALID_START_DATE_SEC = 788889600l;

	public final static String VALID_END_DATE = "2050-12-31 00:00:00";
	// 2050-12-31 23:59:59=2556115199
	public final static long VALID_END_DATE_SEC = 2556115199l;
	
	/**
	 * 2、新增一个处理链，catm、latm、fitm本身有时间但异常的，直接放空。 时间在1995年以前，2050年以后的直接清空
	 * 
	 * @author daihb 20180327 业务时间
	 */
	public static String checkBusiSeondValid(String strSecond) {
		try {
			if (!StringUtils.isNumeric(strSecond)) {
				return "0";
			}
			long second = Long.valueOf(strSecond);
			if (second >= VALID_START_DATE_SEC && second <= VALID_END_DATE_SEC) {
				return strSecond;
			} 
			
			return "0";
		} catch (Exception e) {
			return "0";
		}		
	}
	
	/**
	 * 2、新增一个处理链，catm、latm、fitm本身有时间但异常的，直接放空。 时间在1995年以前，2050年以后的直接清空
	 * 
	 * @author daihb 20180327 业务时间
	 */
	public static Date checkBusiDateValid(Date busiDate) {
		if(busiDate==null){
			return null;
		}
		
		long secondOfDate=busiDate.getTime()/1000;
		String ret=checkBusiSeondValid(secondOfDate+"");
		if("0".equals(ret)){
			return null;
		}		
		return busiDate;
	}

    /**
     *
     * 解析字符串带有时区 字符
     * eg:"2017-12-25 17:46:25+08"
     *
     * @param timeZoneDateStr
     * @param timeZoneId
     * @throws ParseException 暴露
     * @return
     */
    public static Date parserTimeZoneDate(final String timeZoneDateStr, String timeZoneId) throws ParseException {
        String timeZoneID = "GMT+8";
        if (timeZoneId != null) {
            timeZoneID = timeZoneId;
        }
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneID));// 东八区
        return dateFormat.parse(timeZoneDateStr);
    }

	/**
	 * 校验字符串日期格式
	 * @param dateString
	 * @param pattern
	 * @return
	 */
	public static boolean checkDateString(String dateString, String pattern) {
		Date date = null;
		try {
			date = convertToDate(dateString, pattern);
		} catch (Exception e) {
		}
		return (date != null);
	}

	public static Date convertToDate(String dateString,String pattern) {
		return JodaTimeUtil.time(dateString, pattern).toDate();
	}

	/**
	 * 计算两个日期之间的差值
	 * @param field  使用Calendar中的常量字段
	 * @param k 日期一
	 * @param d 日期二
	 * @return
	 */
	public static int diffDate(int field, Date k, Date d) {
		int diffnum = 0;
		int needdiff = 0;
		switch (field) {
			case Calendar.SECOND: {
				needdiff = 1000;
				break;
			}
			case Calendar.MINUTE: {
				needdiff = 60 * 1000;
				break;
			}
			case Calendar.HOUR: {
				needdiff = 60 * 60 * 1000;
				break;
			}
			case Calendar.DATE: {
				needdiff = 24 * 60 * 60 * 1000;
				break;
			}
		}
		if (needdiff != 0) {
			diffnum = (int) (d.getTime() / needdiff) - (int) (k.getTime() / needdiff);
		}

		return diffnum;
	}

	public static String formatDateTimeForTimeZone(Date date, TimeZone timeZone) {
		return (formatDateForTimeZone(date, "yyyy-MM-dd HH:mm:ss", timeZone));
	}

	public static String formatDateForTimeZone(Date date, String pattern, TimeZone timeZone) {
		if (date == null)
			return "";
		if (pattern == null)
			pattern = "yyyy-MM-dd";
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		if (timeZone == null) {
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		} else {
			sdf.setTimeZone(timeZone);
		}
		return (sdf.format(date));
	}

}
