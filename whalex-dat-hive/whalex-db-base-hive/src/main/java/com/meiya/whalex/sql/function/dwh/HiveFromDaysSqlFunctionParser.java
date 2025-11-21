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
@SqlFunction(functionName = "FROM_DAYS")
public class HiveFromDaysSqlFunctionParser implements SqlFunctionParser {

    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("DATE_ADD(TO_DATE('0000-01-01'), ");
        String days = operandStrList.get(0);
        sb.append(days).append(")");
        return sb.toString();
    }
}
