package com.meiya.whalex.sql.function;

import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/10
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "CAST")
public class CastSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //函数格式CAST(value AS dataType)
        String value = operandStrList.get(0);
        String dataType = operandStrList.get(1);
        return funcName + "(" + value + " AS " + dataType + ")";
    }
}
