package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/18
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "STR_TO_DATE")
public class PostgreStrToDateFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String format = operandStrList.get(1);
        String fmt = StringUtils.replaceEach(format, new String[]{"%Y","%y","%m","%M","%d","%H","%h","%I","%i","%S","%s"}, new String[]{"YYYY","YY","MM","MONTH","DD","HH24","HH","MI","MI","SS","SS"});
        sb.append(operandStrList.get(0)).append("::DATE, 'YYYY-MM-DD'");
        return  "TO_CHAR(" + sb.toString() + ")";
    }
}
