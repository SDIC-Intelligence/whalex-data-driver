package com.meiya.whalex.db.entity.ani;

import com.meiya.whalex.db.module.ani.PostgreSqlParseHandler;
import com.meiya.whalex.db.sql.function.ani.KingBaseJsonValueSqlFunctionParser;
import com.meiya.whalex.sql.function.SqlFunctionParser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KingBaseSqlParseHandler extends PostgreSqlParseHandler {

    /**
     * 函数解析器容器
     */
    private static Map<String, SqlFunctionParser> kingBaseFunctionParserMap = new ConcurrentHashMap<>();

    public KingBaseSqlParseHandler(BasePostGreDatabaseInfo database) {
        super(database);
        registerFunctionParser(kingBaseFunctionParserMap, new KingBaseJsonValueSqlFunctionParser());
    }

    @Override
    protected SqlFunctionParser getCustomFuncHandler(String funcName) {
        SqlFunctionParser sqlFunctionParser = kingBaseFunctionParserMap.get(funcName);
        if(sqlFunctionParser != null) {
            return sqlFunctionParser;
        }
        return super.getCustomFuncHandler(funcName);
    }
}
