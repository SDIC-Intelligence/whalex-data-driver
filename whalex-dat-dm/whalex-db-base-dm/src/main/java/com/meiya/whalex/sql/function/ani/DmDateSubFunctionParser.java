package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.exception.BusinessException;
import com.meiya.whalex.sql.annotation.SqlFunction;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/11
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATE_SUB")
public class DmDateSubFunctionParser extends DateTimeIntervalFunctionParser {

    @Override
    public String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        StringBuilder sb = new StringBuilder();
        String filed = operandStrList.get(0);
        String interval = operandStrList.get(1);
        interval = StringUtils.removeStartIgnoreCase(interval, "interval");

        String[] split = StringUtils.split(interval, " ");
        String number = StringUtils.replace(split[0], "'", "");
        if (!StringUtils.startsWithIgnoreCase(number, "-")) {
            number = "-" + number;
        } else {
            number = StringUtils.substringAfter(number, "-");
        }
        number = "'" + number + "'";
        String type = split[1];
        String expr = null;
        if (split.length > 2) {
            if (!StringUtils.equals(split[2], "*")) {
                throw new BusinessException("DM 解析 DATE_SUB 函数异常! 无法识别: [" + operandStrList.get(1) + "] 语法!");
            }
            StringBuilder exprSb = new StringBuilder();
            if (!StringUtils.equals(split[3], "-")) {
                exprSb.append("-");
            }
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
