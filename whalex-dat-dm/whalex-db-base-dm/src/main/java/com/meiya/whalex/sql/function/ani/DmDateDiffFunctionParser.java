package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DATE_DIFF")
public class DmDateDiffFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        //时间单位是关键字，需要去双引号
        String timeUnit = operandStrList.get(0);
        timeUnit = timeUnit.replaceAll("\"", "");
        operandStrList.set(0, timeUnit);

        StringBuilder sb = new StringBuilder();
        for (String param : operandStrList) {
            if(sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(param);
        }
        return "DATEDIFF(" + sb.toString() + ")";
    }
}
