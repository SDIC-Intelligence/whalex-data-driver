package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.FindStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;

import java.util.List;
import java.util.Queue;

public class FindParser extends AbstractParser {

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        FindStatement findStatement = new FindStatement(collectionName, statement);

        //query, projection
        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size){
            case 0:
                break;
            case 2:
                String projectionStr = methodParams.get(1);
                BasicDBObject projection = BasicDBObject.parse(projectionStr);
                findStatement.setProjection(projection);
            case 1:
                String queryStr = methodParams.get(0);
                BasicDBObject query = BasicDBObject.parse(queryStr);
                findStatement.setQuery(query);
                break;
            default:
                throw new RuntimeException("find方法参数，最多只能有两个["+statement+"]");
        }

        while (!queue.isEmpty()) {
            String operator = queue.poll();
            methodParams = getMethodParams(queue, statement);
            switch (operator) {
                case "batchSize":
                    findStatement.setBatchSize(getInt(methodParams, operator, statement));
                    break;
                case "skip":
                    findStatement.setSkip(getInt(methodParams, operator, statement));
                    break;
                case "limit":
                    findStatement.setLimit(getInt(methodParams, operator, statement));
                    break;
                case "sort":
                    if(methodParams.size() != 1) {
                        throw new RuntimeException(operator + "方法参数只能是一个["+statement+"]");
                    }
                    findStatement.setSort(BasicDBObject.parse(methodParams.get(0)));
                    break;
                case "hint":
                    if(methodParams.size() != 1) {
                        throw new RuntimeException(operator + "方法参数只能是一个["+statement+"]");
                    }
                    findStatement.setHint(getValue(methodParams.get(0)));
                    break;
                case "max":
                    if(methodParams.size() == 1) {
                        findStatement.setMax(BasicDBObject.parse(methodParams.get(0)));
                    }else if(methodParams.isEmpty()) {
                        findStatement.setMax(new BasicDBObject());
                    }else {
                        throw new RuntimeException(operator + "方法参数最多只能有一个["+statement+"]");
                    }
                    break;
                case "min":
                    if(methodParams.size() == 1) {
                        findStatement.setMin(BasicDBObject.parse(methodParams.get(0)));
                    }else if(methodParams.isEmpty()) {
                        findStatement.setMin(new BasicDBObject());
                    }else {
                        throw new RuntimeException(operator + "方法参数最多只能有一个["+statement+"]");
                    }
                case "count":
                    findStatement.setCount(true);
                    break;
                default:
                    throw new RuntimeException("未知的方法:"  + operator + "["+statement+"]");
            }
        }

        return findStatement;
    }

    private Integer getInt(List<String> methodParams, String method, String statement) {
        if(methodParams.size() != 1) {
            throw new RuntimeException(method + "方法参数只能是一个["+statement+"]");
        }
        return Integer.valueOf(methodParams.get(0));
    }



}
