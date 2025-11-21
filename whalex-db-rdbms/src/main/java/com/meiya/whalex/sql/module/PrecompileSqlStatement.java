package com.meiya.whalex.sql.module;

import java.util.List;

/**
 * sql预编译
 */
public interface PrecompileSqlStatement {

    String getSql();

    void paramHandle(List<Object> params);

}
