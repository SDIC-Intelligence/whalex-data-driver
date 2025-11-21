package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.BulkWriteStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class BulkWriteParser extends AbstractParser {


    private List<WriteModel<Document>> getWriteModels(String arrayJsonStr) {
        List<String> list = strToList(arrayJsonStr);
        List<WriteModel<Document>> writeModelList = new ArrayList<>();

        for (String json : list) {
            Document parse = Document.parse(json);
            String operator = parse.keySet().iterator().next();
            Document document = (Document) parse.get(operator);
            switch (operator) {
                case "insertOne":
                    writeModelList.add(new InsertOneModel(document));
                    break;
                case "updateOne":
                    Document updateFilter = (Document) document.get("filter");
                    Document update = (Document) document.get("update");
                    UpdateOneModel updateOneModel = new UpdateOneModel(updateFilter, update);
                    writeModelList.add(updateOneModel);
                    break;
                case "updateMany":
                    Document updateManyFilter = (Document) document.get("filter");
                    Document updateMany = (Document) document.get("update");
                    UpdateManyModel updateManyModel = new UpdateManyModel(updateManyFilter, updateMany);
                    writeModelList.add(updateManyModel);
                    break;
                case "deleteOne":
                    Document deleteFilter = (Document) document.get("filter");
                    DeleteOneModel deleteOneModel = new DeleteOneModel(deleteFilter);
                    writeModelList.add(deleteOneModel);
                    break;
                case "deleteMany":
                    Document deleteManyFilter = (Document) document.get("filter");
                    DeleteManyModel deleteManyModel = new DeleteManyModel(deleteManyFilter);
                    writeModelList.add(deleteManyModel);
                    break;
                case "replaceOne":
                    Document replaceFilter = (Document) document.get("filter");
                    Document replacement = (Document) document.get("replacement");
                    ReplaceOneModel replaceOneModel = new ReplaceOneModel(replaceFilter, replacement);
                    writeModelList.add(replaceOneModel);
                    break;
                default:
                    throw new RuntimeException("未知的操作类型：" + operator);
            }
        }

        return writeModelList;
    }

    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        BulkWriteStatement bulkWriteStatement = new BulkWriteStatement(collectionName, statement);

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();
        switch (size) {
            case 2:
                BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
                Object bypassDocumentValidation = basicDBObject.get("bypassDocumentValidation");
                Object ordered = basicDBObject.get("ordered");
                BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
                if(bypassDocumentValidation != null) {
                    bulkWriteOptions.bypassDocumentValidation(Boolean.getBoolean(bypassDocumentValidation.toString()));
                }
                if(ordered != null) {
                    bulkWriteOptions.ordered(Boolean.getBoolean(ordered.toString()));
                }
                bulkWriteStatement.setBulkWriteOptions(bulkWriteOptions);
            case 1:
                List<WriteModel<Document>> writeModels = getWriteModels(methodParams.get(0));
                bulkWriteStatement.setWriteModels(writeModels);
                break;
            default:
                throw new RuntimeException("bulkWrite方法参数异常["+statement+"]");
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return bulkWriteStatement;
    }





}
