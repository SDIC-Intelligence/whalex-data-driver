package com.meiya.whalex.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class DatParameterMetaData implements ParameterMetaData {

    private Object[] params;

    public DatParameterMetaData(Object[] params) {
        this.params = params;
    }

    @Override
    public int getParameterCount() throws SQLException {
        if(params == null) return 0;
        return params.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        throw new RuntimeException("方法未实现isNullable");
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        throw new RuntimeException("方法未实现isSigned");
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        throw new RuntimeException("方法未实现getPrecision");
    }

    @Override
    public int getScale(int param) throws SQLException {
        throw new RuntimeException("方法未实现getScale");
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        throw new RuntimeException("方法未实现getParameterType");
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        throw new RuntimeException("方法未实现getParameterTypeName");
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        throw new RuntimeException("方法未实现getParameterClassName");
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        throw new RuntimeException("方法未实现getParameterMode");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new RuntimeException("方法未实现unwrap");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new RuntimeException("方法未实现isWrapperFor");
    }
}
