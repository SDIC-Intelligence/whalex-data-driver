package com.meiya.whalex.sql.function;

import com.meiya.whalex.sql.annotation.SqlFunction;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/9
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "TRIM")
public class TrimSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //解析完后是三个参数  TRIM(BOTH, ' ', `name`) 边，去掉的字符串，字段
        //该函数只需要转化成 TRIM(`name`) 格式
        return funcName + "(" + operandStrList.get(2) + ")";
    }
}
