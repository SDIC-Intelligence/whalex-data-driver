package com.meiya.whalex.sql.function.dwh;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.util.date.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "TIMESTAMPDIFF")
public class HiveTimestampDiff2SqlFunctionParser implements SqlFunctionParser {

    private static List<String> datePatternList = new ArrayList<>();

    static {
        datePatternList.add(DatePattern.NORM_YEAR_PATTERN);
        datePatternList.add(DatePattern.NORM_MONTH_PATTERN);
        datePatternList.add(DatePattern.NORM_DATE_PATTERN);
        datePatternList.add(DatePattern.NORM_TIME_PATTERN);
        datePatternList.add("yyyy-MM-dd HH");
        datePatternList.add(DatePattern.NORM_DATETIME_MINUTE_PATTERN);
        datePatternList.add(DatePattern.NORM_DATETIME_PATTERN);
    }

    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //时间单位是关键字，需要去飘号
        if (CollectionUtil.isNotEmpty(operandStrList) && operandStrList.size() == 3) {
            String timeUnit = operandStrList.get(0);
            timeUnit = timeUnit.replaceAll("`", "");
            switch (timeUnit.toLowerCase()) {
                case "second":
                    return getFunction(operandStrList, null);
                case "minute":
                    return getFunction(operandStrList, 60L);
                case "house":
                    return getFunction(operandStrList, 60L * 60L);
                case "day":
                    StringBuilder sb = new StringBuilder();
                    extractedFunParams(operandStrList, sb);
                    return "DATEDIFF(" + sb.toString() + ")";
                case "month":
                    StringBuilder monthSb = new StringBuilder();
                    extractedFunParams(operandStrList, monthSb);
                    return "FLOOR(MONTHS_BETWEEN(" + monthSb.toString() + "))";
                case "year":
                    StringBuilder yearSb = new StringBuilder("(");
                    String start = operandStrList.get(1);
                    String end = operandStrList.get(2);
                    String startDatePattern = getDatePattern(start);
                    String endDatePattern = getDatePattern(end);
                    if (StringUtils.isNotBlank(endDatePattern)) {
                        yearSb.append("year('").append(end).append("')");
                    } else {
                        yearSb.append("year(").append(end).append(")");
                    }
                    yearSb.append(" - ");
                    if (StringUtils.isNotBlank(startDatePattern)) {
                        yearSb.append("year('").append(start).append("')");
                    } else {
                        yearSb.append("year(").append(start).append(")");
                    }
                    yearSb.append(")");
                    return yearSb.toString();
                default:
                    throw new RuntimeException("无法解析当前函数时间单位类型: " + timeUnit);
            }
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = operandStrList.size() - 1; i >= 0; i--) {
                String dateStr = operandStrList.get(i);
                if(sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(dateStr);
            }
            return "DATEDIFF(" + sb.toString() + ")";
        }
    }

    private void extractedFunParams(List<String> operandStrList, StringBuilder sb) {
        for (int i = operandStrList.size() - 1; i >= 1; i--) {
            String dateStr = operandStrList.get(i);
            if(sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(dateStr);
        }
    }

    /**
     * 获取实际执行函数
     *
     * @param operandStrList
     * @param time
     * @return
     */
    private String getFunction(List<String> operandStrList, Long time) {
        // 分钟差 转换为 (end - start) / 60
        String start = operandStrList.get(1);
        String end = operandStrList.get(2);
        String startDatePattern = getDatePattern(StringUtils.replace(start, "'", ""));
        String endDatePattern = getDatePattern(StringUtils.replace(end, "'", ""));
        StringBuilder functionSb = new StringBuilder();
        if (time != null) {
            functionSb.append("floor((");
        } else {
            functionSb.append("(");
        }
        // 提取时间
        extractedFunParams(end, endDatePattern, functionSb);
        functionSb.append(" - ");
        extractedFunParams(start, startDatePattern, functionSb);
        functionSb.append(")");
        if (time != null) {
            functionSb.append(" / ").append(time).append(")");
        }
        return functionSb.toString();
    }

    /**
     * 组装单个时间
     *
     * @param end
     * @param endDatePattern
     * @param functionSb
     */
    private static void extractedFunParams(String end, String endDatePattern, StringBuilder functionSb) {
        if (StringUtils.isNotBlank(endDatePattern)) {
            // 说明是个时间类型
            functionSb.append("UNIX_TIMESTAMP(")
                    .append(end)
                    .append(",'")
                    .append(endDatePattern)
                    .append("')");
        } else {
            // 可能是个函数 比如 now()
            if (StringUtils.equalsIgnoreCase(end, "current_timestamp()")) {
                functionSb.append("unix_timestamp()");
            } else {
                functionSb.append(end);
            }
        }
    }

    /**
     * 获取时间字符串格式
     * @param dateStr
     * @return
     */
    private String getDatePattern(String dateStr) {
        String pattern = null;
        for (String datePattern : datePatternList) {
            boolean checked = checkDatePattern(dateStr, datePattern);
            if (checked) {
                return datePattern;
            }
        }
        return null;
    }

    /**
     * 获取日期字符串格式
     *
     * @param dateStr
     * @param pattern
     * @return
     */
    private boolean checkDatePattern(String dateStr, String pattern) {
        return DateUtil.checkDateString(dateStr, pattern);
    }
}
