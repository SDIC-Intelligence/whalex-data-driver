package com.meiya.whalex.db.builder;

import com.meiya.whalex.interior.db.operation.in.CreateDatabaseParamCondition;

/**
 * @author 黄河森
 * @date 2021/8/3
 * @project whalex-data-driver-back
 */
public class CreateDatabaseBuilder {

    private CreateDatabaseParamCondition createDatabaseParamCondition;

    private CreateDatabaseBuilder() {
        this.createDatabaseParamCondition = new CreateDatabaseParamCondition();
    }

    public static CreateDatabaseBuilder builder() {
        return new CreateDatabaseBuilder();
    }

    public CreateDatabaseParamCondition build() {
        return this.createDatabaseParamCondition;
    }

    public CreateDatabaseBuilder dbName(String dbName) {
        this.createDatabaseParamCondition.setDbName(dbName);
        return this;
    }

    public CreateDatabaseBuilder character(String character) {
        this.createDatabaseParamCondition.setCharacter(character);
        return this;
    }

    public CreateDatabaseBuilder collate(String collate) {
        this.createDatabaseParamCondition.setCollate(collate);
        return this;
    }
}
