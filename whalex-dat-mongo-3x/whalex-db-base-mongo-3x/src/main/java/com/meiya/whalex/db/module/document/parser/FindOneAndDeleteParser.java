package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.FindOneAndDeleteStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.FindOneAndDeleteOptions;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class FindOneAndDeleteParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        FindOneAndDeleteStatement findOneAndDeleteStatement = new FindOneAndDeleteStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                FindOneAndDeleteOptions findOneAndDeleteOptions = buildFindOneAndDeleteOptions(basicDBObject);
                findOneAndDeleteStatement.setFindOneAndDeleteOptions(findOneAndDeleteOptions);
            case 1:
                BasicDBObject query = BasicDBObject.parse(methodParams.get(0));
                findOneAndDeleteStatement.setQuery(query);
                break;
            default:
                throw new RuntimeException("findOneAndDelete参数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return findOneAndDeleteStatement;
    }

    private FindOneAndDeleteOptions buildFindOneAndDeleteOptions(BasicDBObject basicDBObject) {
        Object projection = basicDBObject.get("projection");
        Object sort = basicDBObject.get("sort");
        Object maxTimeMS = basicDBObject.get("maxTimeMS");
        Object collation = basicDBObject.get("collation");
        FindOneAndDeleteOptions findOneAndDeleteOptions = new FindOneAndDeleteOptions();
        if(projection != null) {
            findOneAndDeleteOptions.projection((BasicDBObject) projection);
        }
        if(sort != null) {
            findOneAndDeleteOptions.sort((BasicDBObject) sort);
        }
        if(maxTimeMS != null) {
            findOneAndDeleteOptions.maxTime(Long.valueOf(maxTimeMS.toString()), TimeUnit.MILLISECONDS);
        }

        if(collation != null) {
            findOneAndDeleteOptions.collation(buildCollation((BasicDBObject) collation));
        }

        return findOneAndDeleteOptions;

    }


}
