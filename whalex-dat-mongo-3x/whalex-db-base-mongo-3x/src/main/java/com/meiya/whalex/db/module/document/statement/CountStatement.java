package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CountOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class CountStatement extends BaseMongoStatement {

    private String collectionName;

    private BasicDBObject query;

    private CountOptions countOptions;

    public CountStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        long result;
        if(countOptions != null) {
            result = collection.countDocuments(query, countOptions);
        }else {
            result = collection.countDocuments(query);
        }
        Document document = new Document();
        document.put("result", result);
        List<Document> list = new ArrayList<>();
        list.add(document);
        return list;
    }

    public void setQuery(BasicDBObject query) {
        this.query = query;
    }

    public void setCountOptions(CountOptions countOptions) {
        this.countOptions = countOptions;
    }
}
