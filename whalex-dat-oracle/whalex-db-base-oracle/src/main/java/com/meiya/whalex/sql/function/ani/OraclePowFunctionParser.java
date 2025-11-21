package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "POW")
public class OraclePowFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        StringBuilder sb = new StringBuilder();
        sb.append("POWER(")
                .append(operandStrList.get(0)).append(",")
                .append(operandStrList.get(1))
                .append(")");
        return sb.toString();

    }
}
