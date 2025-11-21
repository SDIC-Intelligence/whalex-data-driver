package com.meiya.whalex.sql.function.ani;

import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATE_ADD")
public class OracleDateAddFunctionParser extends OracleDateTimeIntervalFunctionParser {


    @Override
    public String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //date_add(d, n) d时刻加上n天
        String param1 = operandStrList.get(0);
        try{
            String str = param1.replace("'", "");
            DateUtil.parseDate(str);
            param1 = "TO_TIMESTAMP('" + str + "', 'YYYY-MM-DD HH24:MI:SS')";
        }catch (Exception e) {

        }

        return param1 + " + " + operandStrList.get(1);
    }

}
