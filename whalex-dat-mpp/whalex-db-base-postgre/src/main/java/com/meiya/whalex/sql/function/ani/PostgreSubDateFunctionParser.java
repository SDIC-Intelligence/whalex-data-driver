package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "SUBDATE")
public class PostgreSubDateFunctionParser extends PostgreDateTimeIntervalFunctionParser {


    @Override
    public String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //subdate('2022-11-11 17:14:10.0000123', 1) 日期减去n天
        return operandStrList.get(0) + "::TIMESTAMP - " + operandStrList.get(1) + "::INTEGER * INTERVAL ' 1 DAY'";
    }

}
