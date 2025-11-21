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
@SqlFunction(functionName = "ATAN2")
public class HiveAtan2SqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("CASE WHEN ");
        String x = operandStrList.get(0);
        String y = operandStrList.get(1);
        sb.append(x).append(" = 0 AND ").append(y).append(" = 0 THEN NULL");
        sb.append(" ELSE ATAN(").append(y).append("/").append(x).append(") END");
        return sb.toString();
    }
}
