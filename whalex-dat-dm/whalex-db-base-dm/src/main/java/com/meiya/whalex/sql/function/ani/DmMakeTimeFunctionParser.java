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
@SqlFunction(functionName = "MAKETIME")
public class DmMakeTimeFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {

        if(operandStrList.size() != 3) {
            throw new RuntimeException("MAKETIME函数参数必须是3个");
        }

        //给定时，分，秒返回时间
        String hour = operandStrList.get(0);
        String minute = operandStrList.get(1);
        String second = operandStrList.get(2);
        return "(CASE " +
                "WHEN 0 <= " + hour + " AND " + hour + " <= 24 " +
                "AND 0 <= " + minute + " AND " + minute + " <= 60 " +
                "AND 0 <= " + second + " AND " + second + " <= 60 " +
                "THEN CONCAT(" + hour + ", ':', " + minute + ", ':', " + second + ") END)";

    }
}
