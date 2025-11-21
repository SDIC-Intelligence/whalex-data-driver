package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "TRUNCATE")
public class HiveTruncateSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("TRUNC(");
        for (String op : operandStrList) {
            sb.append(op).append(",");
        }
        sb.delete(sb.length() - 1, sb.length());
        sb.append(")");
        return sb.toString();
    }
}
