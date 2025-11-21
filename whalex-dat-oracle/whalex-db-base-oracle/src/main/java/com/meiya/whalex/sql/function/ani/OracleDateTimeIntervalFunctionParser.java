package com.meiya.whalex.sql.function.ani;

import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.List;

/**
 * @author 蔡荣桂
 * @date 2022/11/17
 * 时间间隔函数处理器
 * @project whalex-data-driver
 */
public abstract class OracleDateTimeIntervalFunctionParser implements SqlFunctionParser {


    public abstract String parseDateTimeIntervalFunc(String funcName, List<String> operandStrList, String functionQuantifierName);

    @Override
    public String parseFunc(String funcName, List<String> operandStrList, String functionQuantifierName) {
        // oracle 中时间隔需要加上单引号 INTERVAL - 1 MONTH 需要变成 INTERVAL '-1' MONTH
        // 在 com.meiya.whalex.db.module.ani.OracleSqlParseHandler.sqlIntervalOperatorParse 中处理
        return parseDateTimeIntervalFunc(funcName, operandStrList, functionQuantifierName);
    }
}
