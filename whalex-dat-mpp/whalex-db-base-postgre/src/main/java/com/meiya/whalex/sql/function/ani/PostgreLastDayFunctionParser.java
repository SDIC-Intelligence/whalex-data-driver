package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "LAST_DAY")
public class PostgreLastDayFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //给定日期当月的最后一天的日期
        return "(DATE_TRUNC('MONTH', " + operandStrList.get(0) + "::DATE) + INTERVAL '1 MONTH - 1 DAY')::DATE";
    }
}
