package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_EXISTS")
public class DmJsonExistsFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        sb.append(operandStrList.get(0)).append(",").append(operandStrList.get(1));
        return  "(CASE WHEN JSON_VALUE(" + sb.toString() + ") IS NULL THEN FALSE ELSE TRUE END)";
    }
}
