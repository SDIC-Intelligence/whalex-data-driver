package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DAYOFYEAR")
public class PostgreDayOfYearFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //本年的第几天
        return "EXTRACT(DOY FROM " + operandStrList.get(0) + "::TIMESTAMP)";
    }
}
