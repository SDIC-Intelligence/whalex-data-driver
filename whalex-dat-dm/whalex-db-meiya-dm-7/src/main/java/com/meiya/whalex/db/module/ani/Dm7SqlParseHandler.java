package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.db.entity.ani.BaseDmDatabaseInfo;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.ani.Dm7DateFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.Dm7GroupConcatSqlFunctionParser;
import com.meiya.whalex.sql.function.ani.Dm7SubstringIndexFunctionParser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Dm7SqlParseHandler extends DmSqlParseHandler {

    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> dm7FunctionParserMap = new ConcurrentHashMap<>();

    /**
     * 注册函数解析器
     */
    static {
        registerFunctionParser(dm7FunctionParserMap, new Dm7DateFormatFunctionParser());
        registerFunctionParser(dm7FunctionParserMap, new Dm7GroupConcatSqlFunctionParser());
        registerFunctionParser(dm7FunctionParserMap, new Dm7SubstringIndexFunctionParser());
    }


    public Dm7SqlParseHandler(BaseDmDatabaseInfo database) {
        super(database);
    }

    @Override
    protected SqlFunctionParser getCustomFuncHandler(String funcName) {

        SqlFunctionParser sqlFunctionParser = dm7FunctionParserMap.get(funcName);
        if(sqlFunctionParser != null) {
            return sqlFunctionParser;
        }

        return super.getCustomFuncHandler(funcName);
    }
}
