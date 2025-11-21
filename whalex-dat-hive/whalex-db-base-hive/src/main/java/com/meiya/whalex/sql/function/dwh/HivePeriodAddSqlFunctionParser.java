package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "PERIOD_ADD")
public class HivePeriodAddSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("DATE_FORMAT(ADD_MONTHS(");
        String dateStr = operandStrList.get(0);
        dateStr = StringUtils.replace(dateStr, "'", "");
        String addMonth = operandStrList.get(1);
        addMonth = StringUtils.replace(addMonth, "'", "");
        String year = StringUtils.substring(dateStr, 0, 4);
        String month = StringUtils.substring(dateStr, 4, 6);
        sb.append("'").append(year).append("-").append(month).append("-01', ").append(addMonth).append("), 'yyyyMM')");
        return sb.toString();
    }
}
