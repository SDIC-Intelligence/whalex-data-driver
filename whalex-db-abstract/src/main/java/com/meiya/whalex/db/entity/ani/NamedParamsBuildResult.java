package com.meiya.whalex.db.entity.ani;

import java.util.Arrays;

public class NamedParamsBuildResult {

    private String sql;
    private Object[] params;

    public NamedParamsBuildResult() {
    }

    public NamedParamsBuildResult(String sql, Object[] params) {
        this.sql = sql;
        this.params = params;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Object[] getParams() {
        return params;
    }

    public void setParams(Object[] params) {
        this.params = params;
    }

    @Override
    public String toString() {
        return "NamedParamsBuildresult{" +
                "sql='" + sql + '\'' +
                ", params=" + Arrays.toString(params) +
                '}';
    }
}
