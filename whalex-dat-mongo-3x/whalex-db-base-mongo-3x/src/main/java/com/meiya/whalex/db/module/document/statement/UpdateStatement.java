package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.meiya.whalex.util.JsonUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class UpdateStatement extends BaseMongoStatement {

    private String collectionName;

    private BasicDBObject filter;

    private BasicDBObject update;

    private UpdateOptions updateOptions;

    private boolean multi;

    public UpdateStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        UpdateResult updateResult;
        if(multi) {
            if(updateOptions != null) {
                updateResult = collection.updateMany(filter, update, updateOptions);
            }else {
                updateResult = collection.updateMany(filter, update);
            }
        }else {
            if(updateOptions != null) {
                updateResult = collection.updateOne(filter, update, updateOptions);
            }else {
                updateResult = collection.updateOne(filter, update);
            }
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

    public void setUpdate(BasicDBObject update) {
        this.update = update;
    }

    public void setUpdateOptions(UpdateOptions updateOptions) {
        this.updateOptions = updateOptions;
    }

    public BasicDBObject getFilter() {
        return filter;
    }

    public BasicDBObject getUpdate() {
        return update;
    }

    public UpdateOptions getUpdateOptions() {
        return updateOptions;
    }

    public void setMulti(boolean multi) {
        this.multi = multi;
    }
}
