package com.meiya.whalex.db.builder;

import com.meiya.whalex.interior.db.operation.in.DropDatabaseParamCondition;

/**
 * @author 黄河森
 * @date 2021/8/3
 * @project whalex-data-driver-back
 */
public class DropDatabaseBuilder {

    private DropDatabaseParamCondition dropDatabaseParamCondition;

    private DropDatabaseBuilder() {
        this.dropDatabaseParamCondition = new DropDatabaseParamCondition();
    }

    public static DropDatabaseBuilder builder() {
        return new DropDatabaseBuilder();
    }

    public DropDatabaseParamCondition build() {
        return this.dropDatabaseParamCondition;
    }

    public DropDatabaseBuilder dbName(String dbName) {
        this.dropDatabaseParamCondition.setDbName(dbName);
        return this;
    }
}
