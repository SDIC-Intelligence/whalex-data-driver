package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/18
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_ARRAY")
public class PostgreJsonArraySqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        operandStrList.remove(0);
        for (String param : operandStrList) {
            if(sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(param);
        }
        return "JSON_BUILD_ARRAY(" + sb.toString() + ")";
    }
}
