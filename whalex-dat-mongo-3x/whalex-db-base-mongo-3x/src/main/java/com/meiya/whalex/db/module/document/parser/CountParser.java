package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.CountStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.CountOptions;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class CountParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        CountStatement countStatement = new CountStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                CountOptions countOptions  = buildCountOptions(basicDBObject);
                countStatement.setCountOptions(countOptions);
            case 1:
                BasicDBObject query = BasicDBObject.parse(methodParams.get(0));
                countStatement.setQuery(query);
                break;
            case 0:
                break;
            default:
                throw new RuntimeException("count方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return countStatement;
    }

    private CountOptions buildCountOptions(BasicDBObject basicDBObject) {

        Object hint = basicDBObject.get("hint");
        Object hintString = basicDBObject.get("hintString");
        Object limit = basicDBObject.get("limit");
        Object skip = basicDBObject.get("skip");
        Object maxTimeMS = basicDBObject.get("maxTimeMS");
        Object collation = basicDBObject.get("collation");
        CountOptions countOptions = new CountOptions();
        if(hint != null) {
            countOptions.hint((BasicDBObject) hint);
        }
        if(hintString != null) {
            countOptions.hintString(hintString.toString());
        }
        if(collation != null) {
            countOptions.collation(buildCollation((BasicDBObject) collation));
        }
        if(limit != null) {
            countOptions.limit(Integer.valueOf(limit.toString()));
        }
        if(skip != null) {
            countOptions.skip(Integer.valueOf(skip.toString()));
        }
        if(maxTimeMS != null) {
            countOptions.maxTime(Long.valueOf(maxTimeMS.toString()), TimeUnit.MILLISECONDS);
        }


        return countOptions;
    }


}
