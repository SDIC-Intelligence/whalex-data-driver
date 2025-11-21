package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class FindAndModifyStatement extends BaseMongoStatement {

    private BasicDBObject query;
    private BasicDBObject update;
    private FindOneAndUpdateOptions findOneAndUpdateOptions;
    private String collectionName;

    public FindAndModifyStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        Document oneAndUpdate;
        if(findOneAndUpdateOptions != null) {
            oneAndUpdate = collection.findOneAndUpdate(query, update, findOneAndUpdateOptions);
        }else {
            oneAndUpdate = collection.findOneAndUpdate(query, update);
        }

        List<Document> list = new ArrayList<>();
        if(oneAndUpdate != null) {
            list.add(oneAndUpdate);
        }
        return list;
    }

    public void setQuery(BasicDBObject query) {
        this.query = query;
    }

    public void setUpdate(BasicDBObject update) {
        this.update = update;
    }

    public void setFindOneAndUpdateOptions(FindOneAndUpdateOptions findOneAndUpdateOptions) {
        this.findOneAndUpdateOptions = findOneAndUpdateOptions;
    }
}


