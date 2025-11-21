package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class FindOneAndDeleteStatement extends BaseMongoStatement {

    private BasicDBObject query;
    private FindOneAndDeleteOptions findOneAndDeleteOptions;
    private String collectionName;

    public FindOneAndDeleteStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        Document oneAndDelete;
        if(findOneAndDeleteOptions != null) {
            oneAndDelete = collection.findOneAndDelete(query, findOneAndDeleteOptions);
        }else {
            oneAndDelete = collection.findOneAndDelete(query);
        }
        List<Document> list = new ArrayList<>();
        if(oneAndDelete != null) {
            list.add(oneAndDelete);
        }
        return list;
    }

    public void setQuery(BasicDBObject query) {
        this.query = query;
    }

    public void setFindOneAndDeleteOptions(FindOneAndDeleteOptions findOneAndDeleteOptions) {
        this.findOneAndDeleteOptions = findOneAndDeleteOptions;
    }
}


