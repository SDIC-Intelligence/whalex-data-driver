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
@SqlFunction(functionName = "TIMESTAMPDIFF")
public class OracleTimestampDiffFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //日期单位，需要变成 '大写' 格式
        String datePart = operandStrList.get(0).toUpperCase().replaceAll("\"", "");
        if(!datePart.contains("'")) {
            datePart = "'" + datePart + "'";
        }

        String param1 = operandStrList.get(1);
        try{
            String str = param1.replace("'", "");
            DateUtil.parseDate(str);
            param1 = "TO_TIMESTAMP('" + str + "', 'YYYY-MM-DD HH24:MI:SS')";
        }catch (Exception e) {

        }

        String param2 = operandStrList.get(2);
        try{
            String str = param2.replace("'", "");
            DateUtil.parseDate(str);
            param2 = "TO_TIMESTAMP('" + str + "', 'YYYY-MM-DD HH24:MI:SS')";
        }catch (Exception e) {

        }

        return funcName + "(" + datePart + ", " + param1 + ", " + param2 + ")";
    }
}
