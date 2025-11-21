package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATE_DIFF")
public class PostgreDateDiff2FunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //日期单位，需要变成 '大写' 格式
        String datePart = operandStrList.get(0).toUpperCase().replaceAll("\"", "");
        if(!datePart.contains("'")) {
            datePart = "'" + datePart + "'";
        }
        return "TIMESTAMPDIFF(" + datePart + ", " + operandStrList.get(1) + ", " + operandStrList.get(2) + ")";
    }
}
