package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATEDIFF")
public class DmDateDiff2FunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        //相差的天数  datediff('2022-11-11 17:14:10', '2022-11-10 17:14:10')
        String dateTime1 = operandStrList.get(0);
        String dateTime2 = operandStrList.get(1);
        //比较时参数要反过来
        return "DATEDIFF(DAY, " + dateTime2 + ", " + dateTime1+ ")";
    }
}
