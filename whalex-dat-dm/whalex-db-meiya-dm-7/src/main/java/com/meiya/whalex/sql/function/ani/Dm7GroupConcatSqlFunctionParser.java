package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 黄河森
 * @date 2022/8/5
 * @package com.meiya.whalex.sql.function
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "GROUP_CONCAT")
public class Dm7GroupConcatSqlFunctionParser extends DmGroupConcatSqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        if (StringUtils.equalsIgnoreCase(functionQuantifierName, "DISTINCT")) {
            throw new RuntimeException("DM7 GROUP_CONCAT 不支持 DISTINCT 操作");
        }
        return super.parseFunc(funcName, operandStrList, functionQuantifierName);
    }
}
