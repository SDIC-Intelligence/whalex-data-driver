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
@SqlFunction(functionName = "RIGHT")
public class HiveRightSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("SUBSTRING(");
        String targetStr = operandStrList.get(0);
        String index = operandStrList.get(1);
        sb.append(targetStr).append(", ").append(index).append(", ").append("LENGTH(").append(targetStr).append(")");
        sb.append(")");
        return sb.toString();
    }
}
