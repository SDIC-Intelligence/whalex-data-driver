package com.meiya.whalex.sql.module;

public interface SqlParseHandler {

    /**
     * 执行sql解析
     * @param sql
     * @return
     */
    PrecompileSqlStatement executeSqlParse(String sql);
}
