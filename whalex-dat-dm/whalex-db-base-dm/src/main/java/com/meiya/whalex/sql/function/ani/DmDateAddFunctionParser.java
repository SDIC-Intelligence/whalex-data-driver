package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.exception.BusinessException;
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
@SqlFunction(functionName = "DATE_ADD")
public class DmDateAddFunctionParser extends DateTimeIntervalFunctionParser {
    @Override
    public String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String filed = operandStrList.get(0);
        String interval = operandStrList.get(1);
        interval = StringUtils.removeStartIgnoreCase(interval, "interval");

        String[] split = StringUtils.split(interval, " ");
        String number = "'" + StringUtils.replace(split[0], "'", "") + "'";
        String type = split[1];
        String expr = null;
        if (split.length > 2) {
            if (!StringUtils.equals(split[2], "*")) {
                throw new BusinessException("DM 解析 DATE_SUB 函数异常! 无法识别: [" + operandStrList.get(1) + "] 语法!");
            }
            StringBuilder exprSb = new StringBuilder("-");
            for (int i = 3; i < split.length; i++) {
                exprSb.append(split[i]).append(" ");
            }
            expr = exprSb.toString();
        }
        sb.append("TIMESTAMPADD(").append(type).append(",");

        if (StringUtils.isNotBlank(expr)) {
            sb.append(expr);
        } else {
            sb.append(number);
        }
        sb.append(",").append(filed).append(")");
        return sb.toString();
    }
}
