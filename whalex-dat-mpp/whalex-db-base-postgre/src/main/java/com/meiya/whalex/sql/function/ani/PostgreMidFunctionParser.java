package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.annotation.SqlFunction;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/15
 * @project whalex-data-driver
 */
@SqlFunction(functionName = "MID")
public class PostgreMidFunctionParser implements SqlFunctionParser {
    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        //mid(s,n,len)从字符串s的n位置截取长度为len的子字符串
        return "SUBSTRING(" + operandStrList.get(0) + ", " + operandStrList.get(1) + ", " + operandStrList.get(2) + ")";
    }
}
