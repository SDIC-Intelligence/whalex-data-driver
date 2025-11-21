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
@SqlFunction(functionName = "MAKETIME")
public class HiveMakeTimeSqlFunctionParser implements SqlFunctionParser {

    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("CONCAT_WS(':', '");
        String house = operandStrList.get(0);
        String minute = operandStrList.get(1);
        String second = operandStrList.get(2);
        if (house.length() < 2) {
            house = "0" + house;
        }
        if (minute.length() < 2) {
            minute = "0" + minute;
        }
        if (second.length() < 2) {
            second = "0" + second;
        }
        sb.append(house).append("', '").append(minute).append("', '").append(second).append("')");
        return sb.toString();
    }
}
