package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.meiya.whalex.util.JsonUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class DeleteManyStatement extends BaseMongoStatement {

    private BasicDBObject filter;
    private DeleteOptions deleteOptions;
    private String collectionName;

    public DeleteManyStatement(String collectionName, String statement) {
        super(statement);
        this.collectionName = collectionName;
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        DeleteResult deleteResult;
        if(deleteOptions != null) {
            deleteResult = collection.deleteMany(filter, deleteOptions);
        }else {
            deleteResult = collection.deleteMany(filter);
        }

        List<Document> list = new ArrayList<>();
        if(deleteResult != null) {
            list.add(Document.parse(JsonUtil.objectToStr(deleteResult)));
        }
        return list;
    }

    public void setFilter(BasicDBObject filter) {
        this.filter = filter;
    }

    public void setDeleteOptions(DeleteOptions deleteOptions) {
        this.deleteOptions = deleteOptions;
    }
}


