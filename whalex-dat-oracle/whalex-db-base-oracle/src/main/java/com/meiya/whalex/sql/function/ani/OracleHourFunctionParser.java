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
@SqlFunction(functionName = "HOUR")
public class OracleHourFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //提取时间
        String param1 = operandStrList.get(0);
        try{
            String str = param1.replace("'", "");
            DateUtil.parseDate(str);
            param1 = "TO_TIMESTAMP('" + str + "', 'YYYY-MM-DD HH24:MI:SS')";
        }catch (Exception e) {

        }
        return "EXTRACT(HOUR FROM " + param1 +  ")";
    }
}
