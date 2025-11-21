package com.meiya.whalex.sql.function;

import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_ARRAY")
public class JsonArraySqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String nullBehavior = operandStrList.remove(0);
        for (String param : operandStrList) {
            if(sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(param);
        }
        sb.append(" ").append(nullBehavior);
        return funcName + "(" + sb.toString() + ")";
    }
}
