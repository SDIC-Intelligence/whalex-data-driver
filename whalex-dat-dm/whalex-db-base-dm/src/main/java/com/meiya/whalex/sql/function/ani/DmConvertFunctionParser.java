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
@SqlFunction(functionName = "CONVERT")
public class DmConvertFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String value = operandStrList.get(0);
        //达梦区分大小写时，会加上双引号，需要去掉
        String type = operandStrList.get(1).replaceAll("\"", "");
        //DM的convert()函数中的type在前， value
        sb.append("CONVERT(").append(type).append(" , ").append(value).append(")");
        return sb.toString();
    }
}
