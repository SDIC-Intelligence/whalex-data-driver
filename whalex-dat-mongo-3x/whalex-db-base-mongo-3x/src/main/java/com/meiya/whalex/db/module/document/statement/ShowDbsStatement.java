package com.meiya.whalex.db.module.document.statement;

import com.meiya.whalex.db.entity.document.MongoDatabaseInfo;
import com.mongodb.MongoClient;
import com.mongodb.client.ListDatabasesIterable;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ShowDbsStatement  extends BaseMongoStatement {

    public ShowDbsStatement(String statement) {
        super(statement);
    }

    @Override
    public List<Document> execute(MongoDatabaseInfo mongoDatabaseInfo, MongoClient mongoClient) {
        ListDatabasesIterable<Document> documents = mongoClient.listDatabases();
        List<Document> list = new ArrayList<>();
        for (Document document : documents) {
            list.add(document);
        }
        return list;
    }
}
