package com.meiya.whalex.sql.module;

import java.util.List;

public interface ExecuteSqlMethod<D, S> {

    <T> T execute(D databaseConf, S dbConnect, String sql, List<Object> params) throws Exception;
}
