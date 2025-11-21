package com.meiya.whalex.db.module.document.statement;

public abstract class BaseMongoStatement implements MongoStatement {

    private String statement;

    public BaseMongoStatement(String statement) {
        this.statement = statement;
    }

    @Override
    public String toString() {
        return statement;
    }
}
