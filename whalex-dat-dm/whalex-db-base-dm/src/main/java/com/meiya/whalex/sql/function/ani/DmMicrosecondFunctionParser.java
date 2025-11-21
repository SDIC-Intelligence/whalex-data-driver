package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "MICROSECOND")
public class DmMicrosecondFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //获取微秒数 MICROSECOND(now())
        String dateTime = operandStrList.get(0);
        return "DATEPART(MS, " + dateTime +  ") * 1000";
    }
}
