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
@SqlFunction(functionName = "SUBTIME")
public class HiveSubTimeSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("FROM_UNIXTIME(UNIX_TIMESTAMP(");
        String filed = operandStrList.get(0);
        String addTime = operandStrList.get(1);
        addTime = StringUtils.replace(addTime, "'", "");
        sb.append(filed).append(") - ");
        String[] split = StringUtils.split(addTime, ":");
        if (split.length == 1) {
            // 秒
            sb.append(Integer.parseInt(split[0]));
        } else if (split.length == 2) {
            // 分秒
            String minute = split[0];
            String second = split[1];
            sb.append("(").append(Integer.parseInt(minute) * 60).append(" + ").append(Integer.parseInt(second)).append(")");
        } else if (split.length == 3) {
            // 时分秒
            String house = split[0];
            String minute = split[1];
            String second = split[2];
            sb.append("(").append(Integer.parseInt(house) * 60 * 60).append(" + ").append(Integer.parseInt(minute) * 60).append(" + ").append(Integer.parseInt(second)).append(")");
        }
        sb.append(")");
        return sb.toString();
    }
}
