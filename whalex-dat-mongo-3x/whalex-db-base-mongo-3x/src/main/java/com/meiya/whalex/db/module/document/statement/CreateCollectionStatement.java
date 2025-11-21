package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class CreateCollectionStatement extends BaseMongoStatement {

    private String collectionName;

    private CreateCollectionOptions createCollectionOptions;

    public CreateCollectionStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());

        if(createCollectionOptions == null) {
            database.createCollection(collectionName);
        }else {
            database.createCollection(collectionName, createCollectionOptions);
        }



        return new ArrayList<>();
    }

    public void setCreateCollectionOptions(CreateCollectionOptions createCollectionOptions) {
        this.createCollectionOptions = createCollectionOptions;
    }
}
