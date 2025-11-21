package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.FindOneStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;

import java.util.List;
import java.util.Queue;

public class FindOneParser extends AbstractParser {

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        FindOneStatement findOneStatement = new FindOneStatement(collectionName, statement);

        //query, projection
        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size){
            case 0:
                break;
            case 2:
                String projectionStr = methodParams.get(1);
                BasicDBObject projection = BasicDBObject.parse(projectionStr);
                findOneStatement.setProjection(projection);
            case 1:
                String queryStr = methodParams.get(0);
                BasicDBObject query = BasicDBObject.parse(queryStr);
                findOneStatement.setQuery(query);
                break;
            default:
                throw new RuntimeException("find方法参数，最多只能有两个["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return findOneStatement;
    }

}
