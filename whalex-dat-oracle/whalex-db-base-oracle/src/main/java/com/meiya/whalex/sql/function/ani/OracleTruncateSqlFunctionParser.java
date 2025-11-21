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
@SqlFunction(functionName = "TRUNCATE")
public class OracleTruncateSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        for (String param : operandStrList) {
            if(sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(param);
        }
        return "TRUNC(" + sb.toString() + ")";
    }
}
