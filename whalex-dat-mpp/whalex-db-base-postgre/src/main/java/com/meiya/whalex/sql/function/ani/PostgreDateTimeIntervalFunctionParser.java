package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.function.SqlFunctionParser;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/17
 * 时间间隔函数处理器
 * @project whalex-data-driver
 */
public abstract class PostgreDateTimeIntervalFunctionParser implements SqlFunctionParser {


    public abstract String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName);

    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        // pg中时间隔需要加上单引号 INTERVAL - 1 MONTH 或 INTERVAL '-1' MONTH -> INTERVAL '-1 MONTH'
        // 这个处理放在 com.meiya.whalex.db.module.ani.PostgreSqlParseHandler.sqlIntervalOperatorParse
        // 处理 operandStrList.get(0) 第一个参数，如果存在空格，说明是复杂的 expr 表达式，需要加上 ()
        String expr = operandStrList.get(0);
        String[] splitStr = expr.trim().split(" ");
        if (splitStr.length > 1) {
            operandStrList.set(0, "(" + expr + ")");
        }
        return parseDateTimeIntervalFunc(funcName, operandStrList, functionQuantifierName);
    }
}
