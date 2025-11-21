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
@SqlFunction(functionName = "TIME_FORMAT")
public class HiveTimeFormatSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String time = operandStrList.get(0);
        String format = operandStrList.get(1);
        if (StringUtils.equalsIgnoreCase(format, "'%r'")) {
            format = "'HH:mm:ss'";
            sb.append("CONCAT(FROM_UNIXTIME(UNIX_TIMESTAMP(").append(time).append(", 'HH:mm:ss'), 'hh:mm:ss'), ' ', (CASE WHEN ").append("UNIX_TIMESTAMP(").append(time).append(", 'HH:mm:ss') > 14400 THEN 'PM' ELSE 'AM' END").append("))");
        } else {
            format = StringUtils.replaceEach(format, new String[]{"%Y","%y","%m","%M","%d","%H","%h","%I","%i","%S","%s"}, new String[]{"YYYY","YY","mm","MM","dd","HH","hh","MM","mm","ss","ss"});
            sb.append("FROM_UNIXTIME(UNIX_TIMESTAMP(").append(time).append(", 'HH:mm:ss'), ").append(format).append(")");
        }
        return sb.toString();
    }
}
