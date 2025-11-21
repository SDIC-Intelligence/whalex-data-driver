package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "CAST")
public class HiveCastSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String value = operandStrList.get(0);
        // 数据类型有 DATE, DATETIME, TIME, CHAR, SIGNED, UNSIGNED, DECIMAL, BINARY
        String type = operandStrList.get(1);
        if (StringUtils.equalsAnyIgnoreCase(type, "SIGNED", "UNSIGNED")) {
            type = "INT";
        } else if (StringUtils.equalsIgnoreCase(type, "CHAR")) {
            type = "STRING";
        } else if (StringUtils.equalsAnyIgnoreCase(type, "DATETIME", "TIME")) {
            type = "TIMESTAMP";
        }
        sb.append("CAST(").append(value).append(" AS ").append(type).append(")");
        return sb.toString();
    }
}
