package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.meiya.whalex.db.module.document.statement.ReplaceOneStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;

import java.util.List;
import java.util.Queue;

public class ReplaceOneParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        ReplaceOneStatement replaceOneStatement = new ReplaceOneStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 3:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(2));
                ReplaceOptions replaceOptions = buildReplaceOptions(basicDBObject);
                replaceOneStatement.setReplaceOptions(replaceOptions);
            case 2:
                BasicDBObject filter = BasicDBObject.parse(methodParams.get(0));
                Document replacement = Document.parse(methodParams.get(1));
                replaceOneStatement.setFilter(filter);
                replaceOneStatement.setReplacement(replacement);
                break;
            default:
                throw new RuntimeException("replaceOne方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return replaceOneStatement;
    }

    private ReplaceOptions buildReplaceOptions(BasicDBObject basicDBObject) {
        Object upsert = basicDBObject.get("upsert");
        Object bypassDocumentValidation = basicDBObject.get("bypassDocumentValidation");
        Object collation = basicDBObject.get("collation");
        ReplaceOptions replaceOptions = new ReplaceOptions();
        if(upsert != null) {
            replaceOptions.upsert(Boolean.valueOf(upsert.toString()));
        }
        if(bypassDocumentValidation != null) {
            replaceOptions.bypassDocumentValidation(Boolean.valueOf(bypassDocumentValidation.toString()));
        }
        if(collation != null) {
            replaceOptions.collation(buildCollation((BasicDBObject) collation));
        }
        return replaceOptions;
    }


}
