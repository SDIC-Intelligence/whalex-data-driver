package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/11
 * @package com.meiya.whalex.db.util.param.impl.ani
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "MAKEDATE")
public class DmMakeDateFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //给定年份和天数,返回日期
        String year = operandStrList.get(0);
        String day = operandStrList.get(1);
        return "ADD_DAYS(CONCAT(" + year + ",'-01-01'), " + day + ")";
    }
}
