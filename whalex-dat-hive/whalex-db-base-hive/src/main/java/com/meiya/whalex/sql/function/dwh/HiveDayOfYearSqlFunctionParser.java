package com.meiya.whalex.sql.function.dwh;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "DAYOFYEAR")
public class HiveDayOfYearSqlFunctionParser implements SqlFunctionParser {

    private static final String BASE_DATE = "'%s-01-01 00:00:00'";

    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder("DATEDIFF(");
        String date = operandStrList.get(0);
        String year = StringUtils.substringBefore(StringUtils.replace(date, "'", ""), "-");
        String baseDate = String.format(BASE_DATE, year);
        sb.append(date).append(",").append(baseDate).append(")");
        return sb.toString();
    }
}
