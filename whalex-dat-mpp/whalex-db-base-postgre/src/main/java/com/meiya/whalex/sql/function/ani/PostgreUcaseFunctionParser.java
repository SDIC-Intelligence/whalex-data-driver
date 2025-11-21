package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "UCASE")
public class PostgreUcaseFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        return "UPPER(" + operandStrList.get(0) + ")";
    }
}
