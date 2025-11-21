package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.InsertStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.InsertOneOptions;
import org.bson.Document;

import java.util.List;
import java.util.Queue;

public class InsertParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        InsertStatement insertStatement = new InsertStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                Object bypassDocumentValidation = basicDBObject.get("bypassDocumentValidation");
                InsertOneOptions insertOneOptions = new InsertOneOptions();
                insertOneOptions.bypassDocumentValidation(Boolean.getBoolean(bypassDocumentValidation.toString()));
                insertStatement.setInsertOneOptions(insertOneOptions);
            case 1:
                Document document = Document.parse(methodParams.get(0));
                insertStatement.setDocument(document);
                break;
            default:
                throw new RuntimeException("insert方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return insertStatement;
    }





}
