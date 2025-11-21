package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class CreateIndexStatement extends BaseMongoStatement {

    private String collectionName;

    private BasicDBObject keys;

    private IndexOptions indexOptions;

    public CreateIndexStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        String result;
        if(indexOptions != null) {
            result = collection.createIndex(keys, indexOptions);
        }else {
            result = collection.createIndex(keys);
        }
        Document document = new Document();
        document.put("result", result);
        List<Document> list = new ArrayList<>();
        list.add(document);
        return list;
    }

    public void setKeys(BasicDBObject keys) {
        this.keys = keys;
    }

    public void setIndexOptions(IndexOptions indexOptions) {
        this.indexOptions = indexOptions;
    }
}
