package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/18
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_VALUE")
public class PostgreJsonValueSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        String param1 =  operandStrList.get(1).replaceAll("\\$\\.", "");
        return operandStrList.get(0) + "::jsonb->" + param1;
    }
}
