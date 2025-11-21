package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.AggregateStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class AggregateParser extends AbstractParser {

    private List<BasicDBObject> getAggregateList(String arrayJsonStr) {

        List<String> list = strToList(arrayJsonStr);

        List<BasicDBObject> aggregateList = new ArrayList<>();

        for (String json : list) {
            aggregateList.add(BasicDBObject.parse(json));
        }

        return aggregateList;
    }

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        List<String> methodParams = getMethodParams(queue, statement);
        if(methodParams.size() != 1) {
            throw new RuntimeException("aggregate方法参数，只支持一个参数["+statement+"]");
        }

        String arrayJsonStr = methodParams.get(0);
        List<BasicDBObject> dbObjects = getAggregateList(arrayJsonStr);
        if(dbObjects.isEmpty()) {
            throw new RuntimeException("aggregate方法参数异常["+statement+"]");
        }
        AggregateStatement aggregateStatement = new AggregateStatement(collectionName, statement);
        aggregateStatement.setAggregateList(dbObjects);

        return aggregateStatement;
    }





}
