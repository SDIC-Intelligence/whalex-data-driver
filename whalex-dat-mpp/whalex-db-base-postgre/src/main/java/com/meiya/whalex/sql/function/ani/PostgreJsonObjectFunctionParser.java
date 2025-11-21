package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/18
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_OBJECT")
@Slf4j
public class PostgreJsonObjectFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        // 移除 标准 SQL 中的 NULL_ON_NULL 和 ABSENT_ON_NULL 操作参数
        operandStrList.remove(0);
        StringBuilder sb = new StringBuilder();
        for (String param : operandStrList) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(param);
        }
        return "JSON_BUILD_OBJECT(" + sb.toString() + ")";
    }
}
