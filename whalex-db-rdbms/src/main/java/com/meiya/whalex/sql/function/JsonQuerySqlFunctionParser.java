package com.meiya.whalex.sql.function;

import com.meiya.whalex.sql.annotation.SqlFunction;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_QUERY")
public class JsonQuerySqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        sb.append(operandStrList.get(0)).append(",").append(operandStrList.get(1))
                .append(" ");
        String[] withStr = StringUtils.split(operandStrList.get(2), "_");
        for (String with : withStr) {
            sb.append(with).append(" ");
        }
        sb.append(operandStrList.get(3)).append(" ON EMPTY")
                .append(" ").append(operandStrList.get(4)).append(" ON ERROR");
        return funcName + "(" + sb.toString() + ")";
    }
}
