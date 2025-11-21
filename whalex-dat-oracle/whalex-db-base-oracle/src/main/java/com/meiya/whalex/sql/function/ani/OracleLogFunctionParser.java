package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "LOG")
public class OracleLogFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        if(operandStrList.size() == 1) {
            return "LN(" + operandStrList.get(0) + ")";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("LOG(")
                .append(operandStrList.get(0)).append(",")
                .append(operandStrList.get(1))
                .append(")");
        return sb.toString();

    }
}
