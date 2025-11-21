package com.meiya.whalex.db.builder;

import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;
import com.meiya.whalex.interior.db.operation.in.UpdateDatabaseParamCondition;

/**
 * @author 黄河森
 * @date 2021/8/3
 * @project whalex-data-driver-back
 */
public class UpdateDatabaseBuilder {

    private UpdateDatabaseParamCondition updateDatabaseParamCondition;

    private UpdateDatabaseBuilder() {
        this.updateDatabaseParamCondition = new UpdateDatabaseParamCondition();
    }

    public static UpdateDatabaseBuilder builder() {
        return new UpdateDatabaseBuilder();
    }

    public UpdateDatabaseParamCondition build() {
        return this.updateDatabaseParamCondition;
    }

    public UpdateDatabaseBuilder dbName(String dbName) {
        this.updateDatabaseParamCondition.setDbName(dbName);
        return this;
    }

    public UpdateDatabaseBuilder newDbName(String newDbName) {
        this.updateDatabaseParamCondition.setNewDbName(newDbName);
        return this;
    }
}
