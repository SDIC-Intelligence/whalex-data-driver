package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.FindAndModifyStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import java.util.List;
import java.util.Queue;

public class FindAndModifyParser extends AbstractParser {

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        FindAndModifyStatement findAndModifyStatement = new FindAndModifyStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        if(methodParams.size() != 1) {
            throw new RuntimeException("findAndModify参数异常: " + statement);
        }

        BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(0));
        BasicDBObject query = (BasicDBObject) basicDBObject.get("query");
        BasicDBObject update = (BasicDBObject) basicDBObject.get("update");
        if(query == null) {
            throw new RuntimeException("query不能为空：" + statement);
        }
        if(update == null) {
            throw new RuntimeException("update不能为空：" + statement);
        }
        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }
        findAndModifyStatement.setQuery(query);
        findAndModifyStatement.setUpdate(update);
        return findAndModifyStatement;
    }





}
