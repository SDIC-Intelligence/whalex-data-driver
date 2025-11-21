package com.meiya.whalex.sql.module;

import java.util.List;

public class DefaultPrecompileSqlStatement implements PrecompileSqlStatement{

    private String sql;

    public DefaultPrecompileSqlStatement(String sql) {
        this.sql = sql;
    }

    @Override
    public String getSql() {
        return sql;
    }

    @Override
    public void paramHandle(List<Object> params) {

    }
}
