package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATE_ADD")
public class PostgreDateAddFunctionParser extends PostgreDateTimeIntervalFunctionParser {


    @Override
    public String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //date_add(d, n) d时刻加上n天
        return operandStrList.get(0) + "::TIMESTAMP + " + operandStrList.get(1);
    }

}
