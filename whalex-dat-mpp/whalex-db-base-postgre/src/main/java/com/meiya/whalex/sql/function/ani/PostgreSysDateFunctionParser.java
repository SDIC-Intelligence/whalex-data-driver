package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/18
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "SYSDATE")
public class PostgreSysDateFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //当前日期+时间
        return "CURRENT_TIMESTAMP";
    }
}
