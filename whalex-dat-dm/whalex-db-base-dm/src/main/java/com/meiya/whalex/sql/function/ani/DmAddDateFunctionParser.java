package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/11
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "ADDDATE")
public class DmAddDateFunctionParser extends DateTimeIntervalFunctionParser {

    @Override
    public String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //adddate(d, n) d时刻加上n天
        return  "DATE_ADD("+operandStrList.get(0)+", "+operandStrList.get(1)+")";
    }
}
