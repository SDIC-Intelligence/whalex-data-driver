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
@SqlFunction(functionName = "DATE_FORMAT")
public class HighGoDateFormatFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String format = operandStrList.get(1);
        String fmt = StringUtils.replaceEach(format, new String[]{"%Y","%y","%m","%M","%d","%H","%h","%I","%i","%S","%s"}, new String[]{"YYYY","YY","MM","MONTH","DD","HH24","HH","HH","MI","SS","SS"});
        sb.append("CAST(").append(operandStrList.get(0)).append(" as timestamp)").append(",").append(fmt);
        return  "TO_CHAR(" + sb.toString() + ")";
    }
}
