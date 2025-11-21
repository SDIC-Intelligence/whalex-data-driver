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
@SqlFunction(functionName = "SUBSTRING_INDEX")
public class Dm7SubstringIndexFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        StringBuilder sb = new StringBuilder();
        String str = operandStrList.get(0);
        String split = operandStrList.get(1);
        String index = operandStrList.get(2);
        sb.append("CASE WHEN ").append(index).append(" > 0 THEN ").append("SUBSTR(").append(str).append(",");
        sb.append("0,").append("INSTR(").append(str).append(",").append(split).append(",1,").append(index).append(") - 1").append(")");
        sb.append(" ELSE SUBSTR(").append(str).append(",");
        sb.append("INSTR(").append(str).append(",").append(split).append(",-1,").append(index).append(") + 1) END");
        return sb.toString();
    }
}
