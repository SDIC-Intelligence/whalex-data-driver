package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.DistinctStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;

import java.util.List;
import java.util.Queue;

public class DistinctParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        DistinctStatement distinctStatement = new DistinctStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                distinctStatement.setQuery(basicDBObject);
            case 1:
                distinctStatement.setField(getValue(methodParams.get(0)));
                break;
            default:
                throw new RuntimeException("distinct方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return distinctStatement;
    }





}
