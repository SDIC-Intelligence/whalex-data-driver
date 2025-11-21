package com.meiya.whalex.sql.function.dwh;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "JSON_CONTAINS")
public class HiveJsonContainsSqlFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("CASE WHEN ");
        // json 对象
        String jsonTarget = operandStrList.get(0);
        // json 包含 key:value
        String jsonContains = operandStrList.get(1);
        sb.append("get_json_object(").append(jsonTarget).append(",");
        // 包含 map 说明是通过函数拼接的 json 对象
        String key;
        String value;
        if (StringUtils.startsWithIgnoreCase(jsonContains, "map")) {
            key = StringUtils.substringBetween(jsonContains, "(", ",");
            value = StringUtils.substringBetween(jsonContains, ",", ")");
        } else {
            key = StringUtils.substringBetween(jsonContains, "{", ":");
            value = StringUtils.substringBetween(jsonContains, ":", "}");
        }
        key = StringUtils.trim(StringUtils.replaceEach(key, new String[]{"'", "\"", "`"}, new String[]{"", "", ""}));
        value = StringUtils.trim(value);
        sb.append("'$.").append(key).append("') = ").append(value).append(" THEN '1' ELSE '0' END");
        return sb.toString();
    }
}
