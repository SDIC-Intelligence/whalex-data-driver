package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.FindOneAndUpdateStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class FindOneAndUpdateParser extends AbstractParser {

    private FindOneAndUpdateOptions buildFindOneAndUpdateOptions(BasicDBObject basicDBObject) {
        Object projection = basicDBObject.get("projection");
        Object sort = basicDBObject.get("sort");
        Object upsert = basicDBObject.get("upsert");
        Object returnDocument = basicDBObject.get("returnDocument");
        Object maxTimeMS = basicDBObject.get("maxTimeMS");
        Object bypassDocumentValidation = basicDBObject.get("bypassDocumentValidation");
        Object collation = basicDBObject.get("collation");
        Object arrayFilters = basicDBObject.get("arrayFilters");
        FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions();
        if(projection != null) {
            findOneAndUpdateOptions.projection((BasicDBObject) projection);
        }
        if(sort != null) {
            findOneAndUpdateOptions.sort((BasicDBObject) sort);
        }
        if(upsert != null) {
            findOneAndUpdateOptions.upsert(Boolean.valueOf(upsert.toString()));
        }
        if(returnDocument != null) {
            findOneAndUpdateOptions.returnDocument(ReturnDocument.valueOf(returnDocument.toString()));
        }
        if(maxTimeMS != null) {
            findOneAndUpdateOptions.maxTime(Long.valueOf(maxTimeMS.toString()), TimeUnit.MILLISECONDS);
        }
        if(bypassDocumentValidation != null) {
            findOneAndUpdateOptions.bypassDocumentValidation(Boolean.valueOf(bypassDocumentValidation.toString()));
        }
        if(collation != null) {
            findOneAndUpdateOptions.collation(buildCollation((BasicDBObject) collation));
        }
        if(arrayFilters != null) {
            findOneAndUpdateOptions.arrayFilters((List<? extends BasicDBObject>) arrayFilters);
        }
        return findOneAndUpdateOptions;
    }

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        FindOneAndUpdateStatement findOneAndUpdateStatement = new FindOneAndUpdateStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);


        int size = methodParams.size();
        switch (size) {
            case 3:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(2));
                FindOneAndUpdateOptions findOneAndUpdateOptions = buildFindOneAndUpdateOptions(basicDBObject);
                findOneAndUpdateStatement.setFindOneAndUpdateOptions(findOneAndUpdateOptions);
            case 2:
                BasicDBObject query = BasicDBObject.parse(methodParams.get(0));
                BasicDBObject update = BasicDBObject.parse(methodParams.get(1));
                findOneAndUpdateStatement.setQuery(query);
                findOneAndUpdateStatement.setUpdate(update);
                break;
            default:
                throw new RuntimeException("findOneAndUpdate参数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return findOneAndUpdateStatement;
    }





}
