package com.meiya.whalex.db.module.document.statement;

public class FindOneAndUpdateStatement extends FindAndModifyStatement {

    public FindOneAndUpdateStatement(String collectionName, String statement) {
        super(collectionName, statement);
    }
}


