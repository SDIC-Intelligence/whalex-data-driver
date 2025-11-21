package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ShowCollectionsStatement  extends BaseMongoStatement {

    public ShowCollectionsStatement(String statement) {
        super(statement);
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        MongoDatabase database = mongoClient.getDatabase(mongoDatabaseInfo.getDatabaseName());
        ListCollectionsIterable<Document> documents = database.listCollections();
        List<Document> list = new ArrayList<>();
        for (Document document : documents) {
            list.add(document);
        }
        return list;
    }
}
