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
@SqlFunction(functionName = "TIME_TO_SEC")
public class HiveTimeToSecSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String times = operandStrList.get(0);
        sb.append("HOUR(FROM_UNIXTIME(UNIX_TIMESTAMP(").append(times).append(", 'HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')) * 3600 + ");
        sb.append("MINUTE(FROM_UNIXTIME(UNIX_TIMESTAMP(").append(times).append(", 'HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')) * 60 + ");
        sb.append("SECOND(FROM_UNIXTIME(UNIX_TIMESTAMP(").append(times).append(", 'HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'))");
        return sb.toString();
    }
}
