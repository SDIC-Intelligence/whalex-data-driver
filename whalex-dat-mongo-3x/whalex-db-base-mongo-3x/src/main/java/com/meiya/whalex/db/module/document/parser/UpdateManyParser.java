package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.meiya.whalex.db.module.document.statement.UpdateManyStatement;
import com.meiya.whalex.db.module.document.statement.UpdateStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.UpdateOptions;

import java.util.List;
import java.util.Queue;

public class UpdateManyParser extends UpdateParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {
        UpdateStatement updateStatement = (UpdateStatement) super.handler(queue, statement, collectionName);
        UpdateManyStatement updateManyStatement = new UpdateManyStatement(collectionName, statement);
        updateManyStatement.setFilter(updateStatement.getFilter());
        updateManyStatement.setUpdate(updateStatement.getUpdate());
        updateManyStatement.setUpdateOptions(updateStatement.getUpdateOptions());
        return updateManyStatement;
    }



}
