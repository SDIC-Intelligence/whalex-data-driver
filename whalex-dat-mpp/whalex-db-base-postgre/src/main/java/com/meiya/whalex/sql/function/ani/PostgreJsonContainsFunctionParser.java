package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/18
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_CONTAINS")
public class PostgreJsonContainsFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        sb.append(operandStrList.get(0)).append("::jsonb @> ").append(operandStrList.get(1)).append("::jsonb");
        return  sb.toString();
    }
}
