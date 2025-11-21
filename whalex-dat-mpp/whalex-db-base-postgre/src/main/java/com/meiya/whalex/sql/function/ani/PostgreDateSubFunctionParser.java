package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATE_SUB")
public class PostgreDateSubFunctionParser extends PostgreDateTimeIntervalFunctionParser {


    @Override
    public String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //date_sub(d, n) d时刻减去n天
        return operandStrList.get(0) + "::TIMESTAMP - " + operandStrList.get(1);
    }

}
