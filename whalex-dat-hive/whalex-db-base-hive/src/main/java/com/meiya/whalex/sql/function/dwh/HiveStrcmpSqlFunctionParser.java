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
@SqlFunction(functionName = "STRCMP")
public class HiveStrcmpSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("CASE WHEN ");
        String s1 = operandStrList.get(0);
        String s2 = operandStrList.get(1);
        sb.append(s1).append(" = ").append(s2).append(" THEN 0");
        sb.append(" WHEN ").append(s1).append(" > ").append(s2).append(" THEN 1");
        sb.append(" WHEN ").append(s1).append(" < ").append(s2).append(" THEN -1");
        sb.append(" ELSE NULL END");
        return sb.toString();
    }
}
