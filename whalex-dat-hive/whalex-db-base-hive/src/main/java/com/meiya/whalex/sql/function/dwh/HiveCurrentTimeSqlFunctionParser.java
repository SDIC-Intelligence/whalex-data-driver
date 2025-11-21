package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "CURRENT_TIME")
public class HiveCurrentTimeSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        return "FROM_UNIXTIME(UNIX_TIMESTAMP(), 'HH:mm:ss')";
    }
}
