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
@SqlFunction(functionName = "SUBDATE")
public class DmSubDateFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        //日期减去n天 subdate(now(), 1)
        StringBuilder sb = new StringBuilder();
        String dateTime = operandStrList.get(0);
        String dayNum = operandStrList.get(1);
        sb.append("TIMESTAMPADD(").append("DAY").append(", -1 * ").append(dayNum).append(",").append(dateTime).append(")");

        return sb.toString();
    }
}
