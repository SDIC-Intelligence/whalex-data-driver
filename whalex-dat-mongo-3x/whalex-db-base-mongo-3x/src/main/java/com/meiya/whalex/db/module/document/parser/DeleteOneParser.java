package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.DeleteOneStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.DeleteOptions;

import java.util.List;
import java.util.Queue;

public class DeleteOneParser extends AbstractParser {

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        DeleteOneStatement deleteOneStatement = new DeleteOneStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                BasicDBObject collationMap = (BasicDBObject) basicDBObject.get("collation");
                DeleteOptions deleteOptions = new DeleteOptions();
                deleteOptions.collation(buildCollation(collationMap));
                deleteOneStatement.setDeleteOptions(deleteOptions);
            case 1:
                BasicDBObject filter = BasicDBObject.parse(methodParams.get(0));
                deleteOneStatement.setFilter(filter);
                break;
            default:
                throw new RuntimeException("deleteOne方法参数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return deleteOneStatement;
    }





}
