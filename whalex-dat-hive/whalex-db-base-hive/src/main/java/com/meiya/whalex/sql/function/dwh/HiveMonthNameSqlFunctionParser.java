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
@SqlFunction(functionName = "MONTHNAME")
public class HiveMonthNameSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("CASE ");
        String datetime = operandStrList.get(0);
        sb.append("WHEN MONTH(").append(datetime).append(") = 1 THEN 'January' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 2 THEN 'February' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 3 THEN 'March' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 4 THEN 'April' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 5 THEN 'May' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 6 THEN 'June' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 7 THEN 'July' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 8 THEN 'August' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 9 THEN 'September' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 10 THEN 'October' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 11 THEN 'November' ");
        sb.append("WHEN MONTH(").append(datetime).append(") = 12 THEN 'December' ");
        sb.append("ELSE NULL END");
        return sb.toString();
    }
}
