package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.exception.BusinessException;
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
@SqlFunction(functionName = "DATE_ADD")
public class HiveDateAddSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String filed = operandStrList.get(0);
        String interval = operandStrList.get(1);
        String type = null;
        if (StringUtils.containsIgnoreCase(interval, "interval")) {
            interval = StringUtils.removeStartIgnoreCase(interval, "interval");
            int typeIndex = StringUtils.lastIndexOf(interval, " ");
            type = StringUtils.substring(interval, typeIndex + 1);
            interval = StringUtils.removeEnd(interval, type);
            interval = StringUtils.trim(interval);
        }
        // 函数名
        String functionName;
        if (StringUtils.startsWithIgnoreCase(interval, "-")) {
            functionName = "date_sub";
            interval = StringUtils.trim(StringUtils.replace(interval, "-", ""));
        } else {
            functionName = "date_add";
        }
        // 单位
        if (StringUtils.isNotBlank(type) && !StringUtils.equalsIgnoreCase(type, "DAY")) {
            throw new BusinessException("Hive DATE_ADD 函数暂时只支持 DAY 单位操作!");
        }

        sb.append(functionName).append("(").append(filed).append(",").append(interval).append(")");

        return sb.toString();
    }
}
