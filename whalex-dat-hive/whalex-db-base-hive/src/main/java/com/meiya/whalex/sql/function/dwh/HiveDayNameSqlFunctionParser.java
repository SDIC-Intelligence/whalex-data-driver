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
@SqlFunction(functionName = "DAYNAME")
public class HiveDayNameSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("CASE ");
        String datetime = operandStrList.get(0);
        sb.append("WHEN DAYOFWEEK(").append(datetime).append(") = 1 THEN 'Sunday' ");
        sb.append("WHEN DAYOFWEEK(").append(datetime).append(") = 2 THEN 'Monday' ");
        sb.append("WHEN DAYOFWEEK(").append(datetime).append(") = 3 THEN 'Tuesday' ");
        sb.append("WHEN DAYOFWEEK(").append(datetime).append(") = 4 THEN 'Wednesday' ");
        sb.append("WHEN DAYOFWEEK(").append(datetime).append(") = 5 THEN 'Thursday' ");
        sb.append("WHEN DAYOFWEEK(").append(datetime).append(") = 6 THEN 'Friday' ");
        sb.append("WHEN DAYOFWEEK(").append(datetime).append(") = 7 THEN 'Saturday' ");
        sb.append("ELSE NULL END");
        return sb.toString();
    }
}
