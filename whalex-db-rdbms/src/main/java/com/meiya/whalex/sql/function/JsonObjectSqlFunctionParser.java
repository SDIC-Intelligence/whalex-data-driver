package com.meiya.whalex.sql.function;

import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_OBJECT")
public class JsonObjectSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String nullBehavior = operandStrList.remove(0);
        for (int i = 0; i < operandStrList.size(); i++) {
            String param = operandStrList.get(i);
            sb.append(param);
            if (i % 2 == 0) {
                sb.append(":");
            } else {
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(" ").append(nullBehavior);
        return funcName + "(" + sb.toString() + ")";
    }
}
