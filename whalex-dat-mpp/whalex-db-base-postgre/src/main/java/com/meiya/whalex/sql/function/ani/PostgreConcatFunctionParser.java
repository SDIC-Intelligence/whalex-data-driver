package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "CONCAT")
public class PostgreConcatFunctionParser implements SqlFunctionParser {


    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        StringBuilder funcBuilder = new StringBuilder();
        funcBuilder.append(funcName).append("(");

        boolean first = true;
        for (String param : operandStrList) {
            if(param.equalsIgnoreCase("?")) {
                param = param + "::varchar";
            }
            if(first) {
                funcBuilder.append(param);
                first = false;
            }else {
                funcBuilder.append(", ").append(param);
            }
        }

        funcBuilder.append(")");
        return  funcBuilder.toString();

    }
}
