package com.meiya.whalex.db.util.common;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.db.constant.DatatypePeriodConstants;
import com.meiya.whalex.db.entity.PeriodTableJsonBean;
import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.exception.ExceptionCode;
import com.meiya.whalex.interior.db.constant.DbResourceEnum;
import com.meiya.whalex.interior.db.constant.PeriodCycleEnum;
import com.meiya.whalex.util.date.JodaTimeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.util.*;
import java.util.regex.Pattern;

@Slf4j
public class DatatypePeriodUtils {

    /**
     * 根据 插入的时间得,周期类型，表名 算出要入的周期表名
     *
     * @param shortTableName
     * @param periodType
     * @param captureTime    绝对秒数，注意，不是毫秒数.如是 Date 取的getTime() 要除1000 , DateTime 不需要
     * @return
     */
    public static String getPeriodTableName(String shortTableName, String periodType, Long captureTime) {
        if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
            return shortTableName;
        }
        Date date = DateUtil.date(captureTime);
        String prefix = shortTableName + DatatypePeriodConstants.CORE_NAME_CONNECT;
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            String subfix = DateUtil.format(date, "yyyyMMdd");
            if (8 != subfix.length()) {
                return null;
            }
            return prefix + subfix;
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            String subfix = DateUtil.format(date, "yyyyMM");
            if (6 != subfix.length()) {
                return null;
            }
            return prefix + subfix;
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            String subfix = DateUtil.format(date, "yyyy");
            if (4 != subfix.length()) {
                return null;
            }
            return prefix + subfix;
        }
        return shortTableName;
    }


    /**
     * 取周期表的第一个周期表
     *
     * @param shortTableName
     * @param periodType
     * @param storePeriodValue
     * @return
     */
    public static String getFirstPeriodTableName(String shortTableName, String periodType, Integer storePeriodValue) {
        if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
            return shortTableName;
        }
        Date today = new Date();
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            return getPeriodTableName(shortTableName, periodType, com.meiya.whalex.util.date.DateUtil.addDay(today, -storePeriodValue + 1).getTime());
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            return getPeriodTableName(shortTableName, periodType, com.meiya.whalex.util.date.DateUtil.addMonth(today, -storePeriodValue + 1).getTime());
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            return getPeriodTableName(shortTableName, periodType, com.meiya.whalex.util.date.DateUtil.addYear(today, -storePeriodValue + 1).getTime());
        }
        throw new RuntimeException("未知的周期类型[" + periodType + "]");
    }

    /**
     * 索引名正则
     *
     * @param shortTableName
     * @param periodType
     * @return
     */
    public static Pattern getPattern(String shortTableName, String periodType) {
        String patternString = getPatternString(shortTableName, periodType);
        if (StringUtils.isBlank(patternString)) {
            return null;
        } else {
            return Pattern.compile(patternString);
        }
    }

    /**
     * 索引名正则
     *
     * @param shortTableName
     * @param periodType
     * @param rolloverTable
     * @return
     */
    public static Pattern getPattern(String shortTableName, String periodType, boolean rolloverTable) {
        String patternString = getPatternString(shortTableName, periodType);
        if (StringUtils.isBlank(patternString)) {
            return null;
        } else {
            if (rolloverTable) {
                patternString = "-\\d+$";
            }
            return Pattern.compile(patternString);
        }
    }

    /**
     * 索引名正则
     *
     * @param shortTableName
     * @param periodType
     * @return
     */
    public static String getPatternString(String shortTableName, String periodType) {
        shortTableName = shortTableName.replace(".", "\\.");
        if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
            return shortTableName;
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            return shortTableName + DatatypePeriodConstants.CORE_NAME_CONNECT + "\\d{4}((0[1-9])|(1[0-2]))([0-3][0-9])$";
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            return shortTableName + DatatypePeriodConstants.CORE_NAME_CONNECT + "\\d{4}((0[1-9])|(1[0-2]))$";
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            return shortTableName + DatatypePeriodConstants.CORE_NAME_CONNECT + "\\d{4}$";
        }
        return null;
    }


    /**
     * 获取日历周期类型
     *
     * @param periodType
     * @return
     */
    public static int getDayType(String periodType) {
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            return Calendar.DATE;
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            return Calendar.MONTH;
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            return Calendar.YEAR;
        }
        return -1;
    }

    /**
     * 获取分片完整名称
     *
     * @param shortTableName
     * @param periodType
     * @param startTime
     * @param endTime
     * @return
     */
    public static List<String> getPeriodTableNameList(String shortTableName, String periodType, Date startTime, Date endTime) {
        List tableNameList = new ArrayList();
        int dayType = getDayType(periodType);
        if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
            tableNameList.add(shortTableName);
            return tableNameList;
        }
        if (dayType == -1) {
            log.warn("shortTableName: [{}] periodType: [{}] 类型不属于分表类型或者无法识别", shortTableName, periodType);
            tableNameList.add(shortTableName);
            return tableNameList;
        }

        if (startTime.after(endTime)) {
            throw new BusinessException(ExceptionCode.DATE_TYPE_TABLE_TIME_STAR_GREATER_END_EXCEPTION);
        }

        // 获取当前周期范围最晚的日期，和最早时间
        Date lastDate = getPeriodLastDay(endTime, periodType);
        Date beginDate = getPeriodBeginDay(startTime, periodType);

        while (lastDate.after(beginDate) || lastDate.equals(beginDate)) {
            String periodTableName = getPeriodTableName(shortTableName, periodType, lastDate.getTime());
            tableNameList.add(periodTableName);
            lastDate = com.meiya.whalex.util.date.DateUtil.add(lastDate, dayType, -1);
        }
        return tableNameList;
    }

    /**
     * 根据 开始时间 和 结束时间 取周期表
     *
     * 时间默认升序排序
     *
     * @param shortTableName
     * @param periodType
     * @param startTime
     * @param endTime
     * @param periodValue
     * @param dbResourceEnum
     * @return
     */
    public static List<String> getPeriodTableNames(String shortTableName, String periodType, String startTime, String endTime, DbResourceEnum dbResourceEnum, Integer periodValue) {
        List<String> tableNameList;
        try {
            if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType) || periodType == null) {
                tableNameList = new ArrayList<>(1);
                tableNameList.add(shortTableName);
                return tableNameList;
            }

            if (StringUtils.isEmpty(startTime) || StringUtils.isEmpty(endTime)) {
                if (periodValue != null) {
                    tableNameList = getPeriodTableNameList(shortTableName, periodType, periodValue, 0);
                    return tableNameList;
                } else {
                    throw new BusinessException(ExceptionCode.DATE_TYPE_TABLE_NULL_EXCEPTION, "存储周期未设置!");
                }
            }
            //上面是不带时间的,下面是带时间的 周期类型获取开始时间变成  表名_2019(如果周期是年,根据开始时间来获取拼接)
            Date startTimeDt = DateUtils.parseDateStrictly(startTime, JodaTimeUtil.DEFAULT_YMD_FORMAT
                    , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                    , JodaTimeUtil.SOLR_TDATE_FORMATE
                    , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                    , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                    , JodaTimeUtil.COMPACT_YMD_FORMAT
                    , JodaTimeUtil.COMPACT_YMDH_FORMAT);
            Date endTimeDt = DateUtils.parseDateStrictly(endTime, JodaTimeUtil.DEFAULT_YMD_FORMAT
                    , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                    , JodaTimeUtil.SOLR_TDATE_FORMATE
                    , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                    , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                    , JodaTimeUtil.COMPACT_YMD_FORMAT
                    , JodaTimeUtil.COMPACT_YMDH_FORMAT);
            tableNameList = getPeriodTableNameList(shortTableName, periodType, startTimeDt, endTimeDt);
            return tableNameList;
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        }
    }

    /**
     * 根据 开始时间 和 结束时间 取周期表。并进行压缩
     *
     * 时间默认升序排序
     *
     * @param shortTableName
     * @param periodType
     * @param startTime
     * @param endTime
     * @param periodValue
     * @return
     */
    public static String getCompressPeriodTableNames(String shortTableName, String periodType, String startTime, String endTime, Integer periodValue) {
        try {
            if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
                return shortTableName;
            }
            if (StringUtils.isEmpty(startTime) || StringUtils.isEmpty(endTime)) {
                if (periodValue != null) {
                    return getCompressPeriodTableNameList(shortTableName, periodType, periodValue, 0);
                } else {
                    throw new BusinessException(ExceptionCode.DATE_TYPE_TABLE_NULL_EXCEPTION, "存储周期未设置!");
                }
            }
            //上面是不带时间的,下面是带时间的 周期类型获取开始时间变成  表名_2019(如果周期是年,根据开始时间来获取拼接)
            Date startTimeDt = DateUtils.parseDateStrictly(startTime, JodaTimeUtil.DEFAULT_YMD_FORMAT
                    , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                    , JodaTimeUtil.SOLR_TDATE_FORMATE
                    , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                    , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                    , JodaTimeUtil.COMPACT_YMD_FORMAT
                    , JodaTimeUtil.COMPACT_YMDH_FORMAT);
            Date endTimeDt = DateUtils.parseDateStrictly(endTime, JodaTimeUtil.DEFAULT_YMD_FORMAT
                    , JodaTimeUtil.DEFAULT_YMDHMS_FORMAT
                    , JodaTimeUtil.SOLR_TDATE_FORMATE
                    , JodaTimeUtil.SOLR_TDATE_RETURN_FORMATE
                    , JodaTimeUtil.COMPACT_YMDHMS_FORMAT
                    , JodaTimeUtil.COMPACT_YMD_FORMAT
                    , JodaTimeUtil.COMPACT_YMDH_FORMAT);
            return PeriodTableRuleUtil.compress(shortTableName, DateTime.of(startTimeDt), DateTime.of(endTimeDt), PeriodCycleEnum.parse(periodType));
        } catch (Exception e) {
            throw new BusinessException(e.getMessage());
        }
    }

    /**
     * 取周期最晚时间
     *
     * @param endDate       时间
     * @param periodType 周期类型
     * @return 周期最开始时间
     */
    public static Date getPeriodLastDay(Date endDate, String periodType) {
        String pattern;
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            pattern = "yyyy-MM-dd";
            return DateUtil.endOfDay(DateUtil.parse(DateUtil.format(endDate, pattern), pattern));
        } else if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            pattern = "yyyy-MM";
            return DateUtil.endOfMonth(DateUtil.parse(DateUtil.format(endDate, pattern), pattern));
        } else if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            pattern = "yyyy";
            return DateUtil.endOfYear(DateUtil.parse(DateUtil.format(endDate, pattern), pattern));
        } else {
            return endDate;
        }
    }

    /**
     * 取周期最早时间
     *
     * @param endDate       时间
     * @param periodType 周期类型
     * @return 周期最开始时间
     */
    public static Date getPeriodBeginDay(Date endDate, String periodType) {
        String pattern;
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            pattern = "yyyy-MM-dd";
            return DateUtil.beginOfDay(DateUtil.parse(DateUtil.format(endDate, pattern), pattern));
        } else if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            pattern = "yyyy-MM";
            return DateUtil.beginOfMonth(DateUtil.parse(DateUtil.format(endDate, pattern), pattern));
        } else if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            pattern = "yyyy";
            return DateUtil.beginOfYear(DateUtil.parse(DateUtil.format(endDate, pattern), pattern));
        } else {
            return endDate;
        }
    }

    public static List<String> getPeriodTableNameList(String shortTableName, PeriodTableJsonBean periodTableJsonBean) {
        return getPeriodTableNameList(shortTableName, periodTableJsonBean.getPeriodType(), periodTableJsonBean.getPrePeriod(), periodTableJsonBean.getBackwardPeriod(), periodTableJsonBean.getStorePeriodValue());
    }

    /**
     * 向前周期与存储周期 ,取最小周期
     *
     * @param shortTableName
     * @param periodType
     * @param prePeriod
     * @param backwardPeriod
     * @param storePeriodValue
     * @return
     */
    public static List<String> getPeriodTableNameList(String shortTableName, String periodType, Integer prePeriod, Integer backwardPeriod, Integer storePeriodValue) {
        if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
            return Collections.singletonList(shortTableName);
        }
        if (storePeriodValue != null) {
            prePeriod = (storePeriodValue > prePeriod) ? prePeriod : storePeriodValue;
        }
        return getPeriodTableNameList(shortTableName, periodType, prePeriod, backwardPeriod);
    }

    /**
     * 根据周期类型，获取所有要创建的表
     *
     * @param shortTableName 原表名
     * @param periodType     周期类型
     * @param prePeriod      向前创建周期
     * @param backwardPeriod 向后创建周期
     * @return
     */
    public static List<String> getPeriodTableNameList(String shortTableName, String periodType, Integer prePeriod, Integer backwardPeriod) {
        List<String> tableList = new LinkedList<>();
        if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
            tableList.add(shortTableName);
            return tableList;
        }
        Date today = new Date();
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            for (int i = backwardPeriod; i > -prePeriod; i--) {
                tableList.add(getPeriodTableName(shortTableName, periodType, com.meiya.whalex.util.date.DateUtil.addDay(today, i).getTime()));
            }
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            for (int i = backwardPeriod; i > -prePeriod; i--) {
                tableList.add(getPeriodTableName(shortTableName, periodType, com.meiya.whalex.util.date.DateUtil.addMonth(today, i).getTime()));
            }
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            for (int i = backwardPeriod; i > -prePeriod; i--) {
                tableList.add(getPeriodTableName(shortTableName, periodType, com.meiya.whalex.util.date.DateUtil.addYear(today, i).getTime()));
            }
        }
        return tableList;
    }

    /**
     * 根据周期类型，获取所有要创建的表，并进行压缩
     *
     * @param shortTableName 原表名
     * @param periodType     周期类型
     * @param prePeriod      向前创建周期
     * @param backwardPeriod 向后创建周期
     * @return
     */
    public static String getCompressPeriodTableNameList(String shortTableName, String periodType, Integer prePeriod, Integer backwardPeriod) {
        if (DatatypePeriodConstants.PERIOD_TYPE_ONLY_ONE.equals(periodType)) {
            return shortTableName;
        }
        DateTime today = DateUtil.date();
        DateTime startDate = null;
        if (DatatypePeriodConstants.PERIOD_TYPE_DAY.equals(periodType)) {
            startDate = DateUtil.offsetDay(today, -prePeriod + 1);
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_MONTH.equals(periodType)) {
            startDate = DateUtil.offsetMonth(today, -prePeriod + 1);
        }
        if (DatatypePeriodConstants.PERIOD_TYPE_YEAR.equals(periodType)) {
            startDate = DateUtil.offset(today, DateField.YEAR, -prePeriod + 1);
        }
        return PeriodTableRuleUtil.compress(shortTableName, startDate, today, PeriodCycleEnum.parse(periodType));
    }

}
