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
@SqlFunction(functionName = "PERIOD_DIFF")
public class DmPeriodDiffFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //返回相差的月份  period_diff('202210', '202206') YYYYMM 年月格式的参数
        return  "MONTHS_BETWEEN(CONCAT(" + operandStrList.get(0) + ", '01'), CONCAT("+operandStrList.get(1)+", '01'))";
    }
}
