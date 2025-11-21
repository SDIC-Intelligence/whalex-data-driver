package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class InsertManyStatement extends BaseMongoStatement {

    private String collectionName;

    private List<Document> documents;

    private InsertManyOptions insertManyOptions;

    public InsertManyStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);

        if(insertManyOptions != null) {
            collection.insertMany(documents, insertManyOptions);
        }else {
            collection.insertMany(documents);
        }
        return new ArrayList<>();
    }

    public void setDocuments(List<Document> documents) {
        this.documents = documents;
    }

    public void setInsertManyOptions(InsertManyOptions insertManyOptions) {
        this.insertManyOptions = insertManyOptions;
    }
}
