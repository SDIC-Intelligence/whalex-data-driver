package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_ARRAY")
public class HiveJsonArraySqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < operandStrList.size(); i++) {
            String op = operandStrList.get(i);
            sb.append(op).append(",");
        }
        sb.delete(sb.length() - 1, sb.length());
        return  "array(" + sb.toString() + ")";
    }
}
