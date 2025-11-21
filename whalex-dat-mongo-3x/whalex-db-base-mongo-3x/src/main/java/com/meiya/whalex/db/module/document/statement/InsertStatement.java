package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class InsertStatement  extends BaseMongoStatement {

    private String collectionName;

    private Document document;

    private InsertOneOptions insertOneOptions;

    public InsertStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);

        if(insertOneOptions != null) {
            collection.insertOne(document, insertOneOptions);
        }else {
            collection.insertOne(document);
        }
        return new ArrayList<>();
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public void setInsertOneOptions(InsertOneOptions insertOneOptions) {
        this.insertOneOptions = insertOneOptions;
    }
}
