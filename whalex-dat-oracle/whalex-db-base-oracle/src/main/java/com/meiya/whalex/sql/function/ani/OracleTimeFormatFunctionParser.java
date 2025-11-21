package com.meiya.whalex.sql.function.ani;

import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/18
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "TIME_FORMAT")
public class OracleTimeFormatFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String format = operandStrList.get(1);
        String fmt = StringUtils.replaceEach(format, new String[]{"%H","%h","%I","%i","%S","%s","%f", "%p", "%r", "%T"},
                new String[]{"HH24","HH","MI","MI","SS","SS","US","AM","HH:MI:SS AM","HH24:MI:SS"});
        String param1 = operandStrList.get(0);
        try{
            if(param1.contains("'")) {
                String str = param1.replace("'", "");
                param1 = "TO_TIMESTAMP('" + str + "', 'YYYY-MM-DD HH24:MI:SS')";
            }
        }catch (Exception e) {

        }
        sb.append(param1).append(",").append(fmt);
        return  "TO_CHAR(" + sb.toString() + ")";
    }
}
