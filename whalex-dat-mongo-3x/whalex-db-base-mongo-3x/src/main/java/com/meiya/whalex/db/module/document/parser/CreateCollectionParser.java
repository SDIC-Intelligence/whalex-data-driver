package com.meiya.whalex.db.module.document.parser;

import com.meiya.whalex.db.module.document.statement.CreateCollectionStatement;
import com.meiya.whalex.db.module.document.statement.MongoStatement;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptionDefaults;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.client.model.ValidationOptions;

import java.util.List;
import java.util.Queue;

public class CreateCollectionParser extends AbstractParser {


    public MongoStatement handler(Queue<String> queue, String statement, String collectionName) {

        List<String> methodParams = getMethodParams(queue, statement);
        int size = methodParams.size();

        if(size != 1 && size != 2) {
            throw new RuntimeException("createCollection方法参数个数异常["+statement+"]");
        }

        collectionName = methodParams.get(0);
        if((collectionName.startsWith("'") && collectionName.endsWith("'"))
                || (collectionName.startsWith("\"") && collectionName.endsWith("\""))) {
            collectionName = collectionName.substring(1, collectionName.length() - 1);
        }
        CreateCollectionStatement createCollectionStatement = new CreateCollectionStatement(collectionName, statement);

        if(size == 2) {
            BasicDBObject basicDBObject = BasicDBObject.parse(methodParams.get(1));
            CreateCollectionOptions createCollectionOptions  = buildCreateCollectionOptions(basicDBObject);
            createCollectionStatement.setCreateCollectionOptions(createCollectionOptions);
        }

        if(!queue.isEmpty()) {
            throw new RuntimeException("未知的指令: " + statement);
        }

        return createCollectionStatement;
    }

    private CreateCollectionOptions buildCreateCollectionOptions(BasicDBObject basicDBObject) {

        Object autoIndex = basicDBObject.get("autoIndex");
        Object maxDocuments = basicDBObject.get("maxDocuments");
        Object capped = basicDBObject.get("capped");
        Object sizeInBytes = basicDBObject.get("sizeInBytes");
        Object usePowerOf2Sizes = basicDBObject.get("usePowerOf2Sizes");
        Object storageEngineOptions = basicDBObject.get("storageEngineOptions");
        Object indexOptionDefaults = basicDBObject.get("indexOptionDefaults");
        Object validationOptions = basicDBObject.get("validationOptions");
        Object collation = basicDBObject.get("collation");

        CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions();
        if(autoIndex != null) {
            createCollectionOptions.autoIndex(Boolean.valueOf(autoIndex.toString()));
        }
        if(maxDocuments != null) {
            createCollectionOptions.maxDocuments(Long.valueOf(maxDocuments.toString()));
        }
        if(capped != null) {
            createCollectionOptions.capped(Boolean.valueOf(capped.toString()));
        }
        if(sizeInBytes != null) {
            createCollectionOptions.sizeInBytes(Long.valueOf(sizeInBytes.toString()));
        }
        if(usePowerOf2Sizes != null) {
            createCollectionOptions.usePowerOf2Sizes(Boolean.valueOf(usePowerOf2Sizes.toString()));
        }
        if(storageEngineOptions != null) {
            createCollectionOptions.storageEngineOptions((BasicDBObject) storageEngineOptions);
        }
        if(indexOptionDefaults != null) {
            createCollectionOptions.indexOptionDefaults(buildIndexOptionDefaults((BasicDBObject) indexOptionDefaults));
        }
        if(validationOptions != null) {
            createCollectionOptions.validationOptions(buildValidationOptions((BasicDBObject) validationOptions));
        }
        if(collation != null) {
            createCollectionOptions.collation(buildCollation((BasicDBObject) collation));
        }

        return createCollectionOptions;
    }

    private IndexOptionDefaults buildIndexOptionDefaults(BasicDBObject basicDBObject) {
        IndexOptionDefaults indexOptionDefaults = new IndexOptionDefaults();
        Object storageEngine = basicDBObject.get("storageEngine");
        if(storageEngine != null) {
            indexOptionDefaults.storageEngine((BasicDBObject)storageEngine);
        }
        return indexOptionDefaults;
    }

    private ValidationOptions buildValidationOptions(BasicDBObject basicDBObject){
        ValidationOptions validationOptions = new ValidationOptions();
        Object validator = basicDBObject.get("validator");
        if(validator != null) {
            validationOptions.validator((BasicDBObject)validator);
        }
        Object validationLevel = basicDBObject.get("validationLevel");
        if(validationLevel != null) {
            validationOptions.validationLevel(ValidationLevel.fromString(validationLevel.toString()));
        }
        Object validationAction = basicDBObject.get("validationAction");
        if(validationAction != null) {
            validationOptions.validationAction(ValidationAction.fromString(validationAction.toString()));
        }
        return validationOptions;
    }

}
