package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "MAKETIME")
public class PostgreMakeTimeFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //计算日期是本年的第几个星期
        return "MAKE_TIME(" + operandStrList.get(0)+", "+operandStrList.get(1) + ", "+operandStrList.get(2) + ")";
    }
}
