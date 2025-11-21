package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "MAKEDATE")
public class HiveMakeDateSqlFunctionParser implements SqlFunctionParser {

    private static final String BASE_DATE = "'%s-01-01'";

    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("DATE_ADD(TO_DATE(");
        String year = operandStrList.get(0);
        String dayOfYear = operandStrList.get(1);
        sb.append(String.format(BASE_DATE, year)).append("), ").append(dayOfYear).append(")");
        return sb.toString();
    }
}
