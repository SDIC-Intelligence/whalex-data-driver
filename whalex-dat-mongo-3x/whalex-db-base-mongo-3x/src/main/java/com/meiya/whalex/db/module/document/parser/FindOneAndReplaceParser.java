package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.FindOneAndReplaceStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class FindOneAndReplaceParser extends AbstractParser {



    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        FindOneAndReplaceStatement findOneAndReplaceStatement = new FindOneAndReplaceStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 3:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(2));
                FindOneAndReplaceOptions findOneAndReplaceOptions = buildFindOneAndReplaceOptions(basicDBObject);
                findOneAndReplaceStatement.setFindOneAndReplaceOptions(findOneAndReplaceOptions);
            case 2:
                BasicDBObject filter = BasicDBObject.parse(methodParams.get(0));
                Document replacement = Document.parse(methodParams.get(1));
                findOneAndReplaceStatement.setFilter(filter);
                findOneAndReplaceStatement.setReplacement(replacement);
                break;
            default:
                throw new RuntimeException("findOneAndReplace参数异常["+statement+"]");
        }


        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return findOneAndReplaceStatement;
    }

    private FindOneAndReplaceOptions buildFindOneAndReplaceOptions(BasicDBObject basicDBObject) {
        Object projection = basicDBObject.get("projection");
        Object sort = basicDBObject.get("sort");
        Object upsert = basicDBObject.get("upsert");
        Object returnDocument = basicDBObject.get("returnDocument");
        Object maxTimeMS = basicDBObject.get("maxTimeMS");
        Object bypassDocumentValidation = basicDBObject.get("bypassDocumentValidation");
        Object collation = basicDBObject.get("collation");
        FindOneAndReplaceOptions findOneAndReplaceOptions = new FindOneAndReplaceOptions();
        if(projection != null) {
            findOneAndReplaceOptions.projection((BasicDBObject) projection);
        }
        if(sort != null) {
            findOneAndReplaceOptions.sort((BasicDBObject) sort);
        }
        if(upsert != null) {
            findOneAndReplaceOptions.upsert(Boolean.valueOf(upsert.toString()));
        }
        if(returnDocument != null) {
            findOneAndReplaceOptions.returnDocument(ReturnDocument.valueOf(returnDocument.toString()));
        }
        if(maxTimeMS != null) {
            findOneAndReplaceOptions.maxTime(Long.valueOf(maxTimeMS.toString()), TimeUnit.MILLISECONDS);
        }
        if(bypassDocumentValidation != null) {
            findOneAndReplaceOptions.bypassDocumentValidation(Boolean.valueOf(bypassDocumentValidation.toString()));
        }
        if(collation != null) {
            findOneAndReplaceOptions.collation(buildCollation((BasicDBObject) collation));
        }

        return findOneAndReplaceOptions;

    }


}
