package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.meiya.whalex.util.JsonUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ReplaceOneStatement extends BaseMongoStatement {

    private String collectionName;

    private BasicDBObject filter;

    private Document replacement;

    private ReplaceOptions replaceOptions;

    public ReplaceOneStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        UpdateResult updateResult;
        if(replaceOptions != null) {
            updateResult = collection.replaceOne(filter, replacement, replaceOptions);
        }else {
            updateResult = collection.replaceOne(filter, replacement);
        }
        List<Document> list = new ArrayList<>();
        if(updateResult != null) {
            list.add(Document.parse(JsonUtil.objectToStr(updateResult)));
        }
        return list;
    }

    public void setFilter(BasicDBObject filter) {
        this.filter = filter;
    }

    public void setReplacement(Document replacement) {
        this.replacement = replacement;
    }

    public void setReplaceOptions(ReplaceOptions replaceOptions) {
        this.replaceOptions = replaceOptions;
    }
}
