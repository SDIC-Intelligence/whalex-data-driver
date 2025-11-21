package com.meiya.whalex.sql.function.ani;

import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "LAST_DAY")
public class OracleLastDayFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //给定日期当月的最后一天的日期
        String param1 = operandStrList.get(0);
        try{
            String str = param1.replace("'", "");
            DateUtil.parseDate(str);
            param1 = "TO_TIMESTAMP('" + str + "', 'YYYY-MM-DD HH24:MI:SS')";
        }catch (Exception e) {

        }
        return funcName + "(" + param1 + ")";
    }
}
