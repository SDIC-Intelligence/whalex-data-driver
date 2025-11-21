package com.meiya.whalex.db.module.ani;

import com.meiya.whalex.sql.function.SqlFunctionParser;
import com.meiya.whalex.sql.function.ani.DorisConvertFunctionParser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 黄河森
 * @date 2024/8/20
 * @package com.meiya.whalex.db.module.ani
 * @project whalex-data-driver
 * @description DorisSqlParseHandler
 */
public class DorisSqlParseHandler extends MysqlSqlParseHandler {


    /**
     * 函数解析器容器
     */
    protected static Map<String, SqlFunctionParser> dorisFunctionParserMap = new ConcurrentHashMap<>();

    public DorisSqlParseHandler() {
        super();
        registerFunctionParser(dorisFunctionParserMap, new DorisConvertFunctionParser());
    }

    @Override
    protected SqlFunctionParser getCustomFuncHandler(String funcName) {
        SqlFunctionParser sqlFunctionParser = dorisFunctionParserMap.get(funcName);
        if(sqlFunctionParser != null) {
            return sqlFunctionParser;
        }
        return super.getCustomFuncHandler(funcName);
    }
}
