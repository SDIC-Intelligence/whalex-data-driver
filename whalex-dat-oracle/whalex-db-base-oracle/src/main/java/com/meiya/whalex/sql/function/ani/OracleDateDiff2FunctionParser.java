package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATEDIFF")
public class OracleDateDiff2FunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        List<String> list = new ArrayList<>(operandStrList.size() + 1);
        list.add("DAY");
        list.addAll(operandStrList);
        return new OracleTimestampDiffFunctionParser().parseFunc("TIMESTAMPDIFF", list, functionQuantifierName);
    }
}
