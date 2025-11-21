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
@SqlFunction(functionName = "SPLIT")
public class MysqlSplitFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String value = operandStrList.get(0);
        String delimiter = operandStrList.get(1);
        String position = operandStrList.get(2);
        sb.append("SUBSTRING_INDEX(SUBSTRING_INDEX(").append(value).append(",")
                .append(delimiter).append(", ").append(position).append("), ").append(delimiter).append(", -1)");
        return sb.toString();
    }
}
