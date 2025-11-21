package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class DropIndexStatement extends BaseMongoStatement {

    private String collectionName;

    private Object index;

    public DropIndexStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        if(index instanceof String) {
            collection.dropIndex(index.toString());
        }else {
            collection.dropIndex((BasicDBObject)index);
        }
        List<Document> list = new ArrayList<>();
        return list;
    }

    public void setIndex(Object index) {
        this.index = index;
    }
}
