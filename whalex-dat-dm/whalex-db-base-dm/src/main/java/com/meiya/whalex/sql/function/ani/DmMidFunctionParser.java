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
@SqlFunction(functionName = "MID")
public class DmMidFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //mid(s,n,len) -> substr(s,n,len)
        //从字符串s的n位置截取长度为len的子字符串
        StringBuilder sb = new StringBuilder();
        for (String param : operandStrList) {
            if(sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(param);
        }
        return "SUBSTR(" + sb.toString() + ")";
    }
}
