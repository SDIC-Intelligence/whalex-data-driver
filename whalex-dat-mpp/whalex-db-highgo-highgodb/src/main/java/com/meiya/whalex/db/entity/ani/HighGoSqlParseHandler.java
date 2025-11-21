package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.module.ani.PostgreSqlParseHandler;
import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.ani.HighGoDateFormatFunctionParser;
import com.meiya.whalex.sql.function.ani.HighGoIfNullFunctionParser;
import com.meiya.whalex.sql.function.ani.HighGoJsonValueSqlFunctionParser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HighGoSqlParseHandler extends PostgreSqlParseHandler {

    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> highgoFunctionParserMap = new ConcurrentHashMap<>();

    public HighGoSqlParseHandler(BasePostGreDatabaseInfo database) {
        super(database);
        registerFunctionParser(highgoFunctionParserMap, new HighGoDateFormatFunctionParser());
        registerFunctionParser(highgoFunctionParserMap, new HighGoIfNullFunctionParser());
        registerFunctionParser(highgoFunctionParserMap, new HighGoJsonValueSqlFunctionParser());
    }

    @Override
    protected SqlFunctionParser getCustomFuncHandler(String funcName) {
        SqlFunctionParser sqlFunctionParser = highgoFunctionParserMap.get(funcName);
        if(sqlFunctionParser != null) {
            return sqlFunctionParser;
        }
        return super.getCustomFuncHandler(funcName);
    }
}
