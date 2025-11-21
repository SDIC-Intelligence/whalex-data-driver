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
@SqlFunction(functionName = "PERIOD_DIFF")
public class HivePeriodDiffSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String beginDateStr = operandStrList.get(0);
        String endDateStr = operandStrList.get(1);
        beginDateStr = StringUtils.replace(beginDateStr, "'", "");
        String beginYear = StringUtils.substring(beginDateStr, 0, 4);
        String beginMonth = StringUtils.substring(beginDateStr, 4, 6);
        endDateStr = StringUtils.replace(endDateStr, "'", "");
        String endYear = StringUtils.substring(endDateStr, 0, 4);
        String endMonth = StringUtils.substring(endDateStr, 4, 6);
        sb.append("(").append(Integer.parseInt(beginYear)).append(" - ").append(Integer.parseInt(endYear)).append(") * 12 + (").append(Integer.parseInt(beginMonth)).append(" - ").append(Integer.parseInt(endMonth)).append(")");
        return sb.toString();
    }
}
