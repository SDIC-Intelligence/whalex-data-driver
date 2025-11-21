package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "SEC_TO_TIME")
public class HiveSecToTimeSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("CONCAT_WS(':', ");
        String seconds = operandStrList.get(0);
        sb.append("LPAD((").append(seconds).append(" DIV 3600) % 60, 2, '0'), ");
        sb.append("LPAD((").append(seconds).append(" DIV 60) % 60, 2, '0'), ");
        sb.append("LPAD(").append(seconds).append(" % 60, 2, '0'))");
        return sb.toString();
    }
}
