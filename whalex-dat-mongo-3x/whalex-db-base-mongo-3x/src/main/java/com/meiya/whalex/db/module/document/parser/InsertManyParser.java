package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.InsertManyStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class InsertManyParser extends AbstractParser {


    private List<Document> getInsertList(String arrayJsonStr) {
        List<String> list = strToList(arrayJsonStr);
        List<Document> insertList = new ArrayList<>();

        for (String json : list) {
            insertList.add(Document.parse(json));
        }

        return insertList;
    }

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        InsertManyStatement insertManyStatement = new InsertManyStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                Object bypassDocumentValidation = basicDBObject.get("bypassDocumentValidation");
                Object ordered = basicDBObject.get("ordered");
                InsertManyOptions insertManyOptions = new InsertManyOptions();
                if(bypassDocumentValidation != null) {
                    insertManyOptions.bypassDocumentValidation(Boolean.getBoolean(bypassDocumentValidation.toString()));
                }
                if(ordered != null) {
                    insertManyOptions.ordered(Boolean.getBoolean(ordered.toString()));
                }
                insertManyStatement.setInsertManyOptions(insertManyOptions);
            case 1:
                List<Document> insertList = getInsertList(methodParams.get(0));
                insertManyStatement.setDocuments(insertList);
                break;
            default:
                throw new RuntimeException("insertMany方法参数个数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return insertManyStatement;
    }





}
