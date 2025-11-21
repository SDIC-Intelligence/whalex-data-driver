package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.DeleteManyStatement;
import com.meiya.whalex.db.module.document.statement.DeleteOneStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.DeleteOptions;

import java.util.Queue;

public class DeleteManyParser extends DeleteOneParser {

    @Override
    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {
        DeleteOneStatement deleteOneStatement = (DeleteOneStatement) super.handler(queue, statement, collectionName);
        DeleteOptions deleteOptions = deleteOneStatement.getDeleteOptions();
        BasicDBObject filter = deleteOneStatement.getFilter();
        DeleteManyStatement deleteManyStatement = new DeleteManyStatement(collectionName, statement);
        deleteManyStatement.setDeleteOptions(deleteOptions);
        deleteManyStatement.setFilter(filter);
        return deleteManyStatement;
    }
}
